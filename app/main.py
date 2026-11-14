"""
遥测恢复演练台（FastAPI）

核心流程（真实物理恢复，非 SQL 重建）：
  1. 复制 pg_basebackup 物理基线备份到独立运行目录
  2. 写入恢复配置：restore_command 指向 WAL 归档、recovery_target_name 精确停在
     所选命名恢复点、recovery_target_action=promote
  3. 以独立 postgres 实例（容器内 127.0.0.1:5433）重放 WAL
  4. 三重证据确认“准确到达目标”后才开放结果：
       - 日志出现 recovery stopping at restore point "<所选恢复点>"
       - 日志出现 archive recovery complete
       - SQL 查询 pg_is_in_recovery() = false（已 promote）
     仅凭数据库可连接一律不算成功
  5. 归档缺段 / 未到达目标 -> FAILED：立即停止并回收恢复进程，残库不开放查询，
     必须显式“回收残库”后才允许重试
  6. 结果开放（READY）不是一次性结论：访问状态/汇总/明细时复核恢复实例存活
       - 确认已停止   -> INVALIDATED：当前任务失效，旧汇总与旧明细一律拒绝
       - 暂无法确认   -> SUSPENDED：结果暂停开放，保留副本与占用，待确认停止并回收
       - 确认仍存活   -> READY：单次连接失败只报查询错误，不误清恢复任务
     判定基于进程（postmaster.pid + pg_ctl status），不能用“连不上数据库”代替
  7. 全站同时只允许一份恢复（状态机 + 锁）
"""
import json
import os
import re
import shutil
import signal
import subprocess
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

import psycopg
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from psycopg.rows import dict_row
from pydantic import BaseModel

PG_BIN = os.environ.get("PG_BIN", "/usr/lib/postgresql/16/bin")
PRIMARY_DSN = os.environ.get(
    "PRIMARY_DSN",
    "host=primary port=5432 dbname=telemetry user=postgres password=postgres",
)
BASEBACKUP_DIR = Path(os.environ.get("BASEBACKUP_DIR", "/basebackup/base"))
MANIFEST_PATH = Path(os.environ.get("MANIFEST_PATH", "/basebackup/manifest.json"))
RUNS_DIR = Path(os.environ.get("RUNS_DIR", "/recovery"))
RESTORE_SCRIPT = os.environ.get("RESTORE_SCRIPT", "/opt/app/restore_command.sh")
RECOVERY_PORT = int(os.environ.get("RECOVERY_PORT", "5433"))
RECOVERY_DB = os.environ.get("RECOVERY_DB", "telemetry")
RECOVERY_USER = os.environ.get("RECOVERY_USER", "postgres")
RECOVERY_PASSWORD = os.environ.get("RECOVERY_PASSWORD", "postgres")
RECOVERY_TIMEOUT = int(os.environ.get("RECOVERY_TIMEOUT", "300"))
STATIC_DIR = Path(__file__).resolve().parent / "static"

IDLE, PREPARING, RECOVERING, VERIFYING, READY, FAILED = (
    "IDLE", "PREPARING", "RECOVERING", "VERIFYING", "READY", "FAILED",
)
# 结果开放后的两种“实例存活”异常态：
#   SUSPENDED   —— 存活探测结果暂时无法确认：结果暂停开放，但保留副本与恢复占用
#   INVALIDATED —— 已确认实例停止：当前任务失效，旧汇总/明细一律拒绝
SUSPENDED, INVALIDATED = "SUSPENDED", "INVALIDATED"
RESULT_STATES = (READY, SUSPENDED)   # 只有这些状态需要做存活探测

# 存活探测结论（三态，任何“连不上数据库”都不能直接当作已停止）
ALIVE, STOPPED, UNKNOWN = "ALIVE", "STOPPED", "UNKNOWN"

STOPPED_REASON = (
    "恢复实例已停止：当前恢复任务失效，旧汇总与旧明细不再提供。"
    "请回收失效副本及占用后再重新发起恢复。"
)
UNKNOWN_REASON = (
    "暂无法确认恢复实例状态：结果已暂停开放，副本与恢复占用保留；"
    "待确认实例停止并完成回收后才允许重新恢复。"
)


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


class Manager:
    """全站单恢复状态机。

    epoch 是结果版本号：任务失效、结果暂停/恢复、创建任务、回收副本时递增。
    汇总/明细请求在返回前必须在锁内复核 epoch，避免并发状态查询判定失效后
    仍然把判定前的旧结果交付出去。
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.state = IDLE
        self.run = None
        self.epoch = 0

    def event(self, msg: str):
        print(f"[recovery] {msg}", flush=True)
        if self.run is not None:
            self.run["events"].append({"ts": now_iso(), "msg": msg})


mgr = Manager()


def load_manifest() -> dict:
    if not MANIFEST_PATH.exists():
        raise HTTPException(503, "setup 尚未完成：未找到 /basebackup/manifest.json")
    try:
        return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        raise HTTPException(500, f"清单解析失败: {e}")


def recovered_dsn(extra: str = "") -> str:
    return (
        f"host=127.0.0.1 port={RECOVERY_PORT} dbname={RECOVERY_DB} "
        f"user={RECOVERY_USER} password={RECOVERY_PASSWORD} connect_timeout=2 {extra}"
    )


def run_cmd(args, timeout=120) -> subprocess.CompletedProcess:
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout)


def instance_pid(pgdata: Path) -> int | None:
    try:
        return int((pgdata / "postmaster.pid").read_text(encoding="utf-8")
                   .splitlines()[0].strip())
    except Exception:
        return None


def postmaster_state(pgdata: Path) -> str:
    """依据 postmaster.pid + /proc 判定进程层状态。

    返回：
      "alive"    —— pid 文件有效，且确为仍运行的 postgres 进程
      "gone"     —— 无 pid 文件 / pid 已不存在 / 僵尸或已退出 / pid 被非 postgres 复用
      "unknown"  —— pid 文件存在且进程存在，但无法读取 /proc 证实身份
    postgres 启动时写 postmaster.pid、退出时删除，因此“无 pid 文件”即“未运行”。
    """
    pid = instance_pid(pgdata)
    if pid is None or pid <= 0:
        return "gone"
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return "gone"
    except PermissionError:
        pass                 # 进程存在但非本用户：继续读 /proc
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8", errors="replace")
        pstate = stat.rsplit(")", 1)[1].split()[0]
    except FileNotFoundError:
        return "gone"
    except Exception:
        return "unknown"
    if pstate in ("Z", "X"):
        return "gone"        # 僵尸/已退出：postmaster 实际已停止
    try:
        cmdline = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ")
    except Exception:
        return "unknown"
    return "alive" if b"postgres" in cmdline else "gone"


def instance_process_alive(pgdata: Path) -> bool:
    """postmaster.pid 指向的进程是否确实是仍存活的恢复实例（防止 pid 复用误判）。"""
    return postmaster_state(pgdata) == "alive"


def pg_ctl_status(pgdata: Path) -> bool | None:
    """pg_ctl status（不依赖 SQL）。返回 True=运行中 / False=已停止 / None=无法执行。"""
    cmd = [f"{PG_BIN}/pg_ctl", "-D", str(pgdata), "status"]
    if os.geteuid() == 0:
        cmd = ["gosu", "postgres"] + cmd
    try:
        return run_cmd(cmd, timeout=10).returncode == 0
    except Exception as e:
        print(f"[recovery] pg_ctl status error: {e}", flush=True)
        return None


def probe_instance(pgdata: Path) -> tuple[str, list[str]]:
    """探测恢复实例存活状态，返回 (结论, 证据列表)。

    关键点：**不能**把“数据库连接失败”直接当作实例已停止。只有进程层能确认
    postmaster 已不在（无 pid 文件 / 进程消失 / 僵尸 / pid 被非 postgres 复用）
    才判 STOPPED。进程确认存活时，单次 SQL 失败只算查询错误（ALIVE）；证据冲突
    或无法读取时才判 UNKNOWN，此时暂停结果开放。
    """
    evidence = []

    pstate = postmaster_state(pgdata)
    pid = instance_pid(pgdata)
    evidence.append(f"postmaster.pid：{pid if pid is not None else '无'}，进程判定：{pstate}")

    pgctl_running = pg_ctl_status(pgdata)
    evidence.append(f"pg_ctl status：{pgctl_running}")

    sql_ok = False
    try:
        with psycopg.connect(recovered_dsn("options='-c statement_timeout=2000'"),
                             autocommit=True, connect_timeout=2) as conn:
            conn.execute("SELECT 1").fetchone()
        sql_ok = True
        evidence.append("SQL 探活成功")
    except psycopg.Error as e:
        evidence.append(f"SQL 探活失败：{type(e).__name__}: {str(e).strip()[:160]}")
    except Exception as e:   # 连接参数/驱动异常也不能当作停止
        evidence.append(f"SQL 探活异常：{type(e).__name__}: {str(e).strip()[:160]}")

    if sql_ok:
        return ALIVE, evidence
    if pstate == "alive":
        return ALIVE, evidence      # 进程仍在：连接失败只是查询错误
    if pstate == "gone":
        return STOPPED, evidence
    # pstate == "unknown"：/proc 无法证实身份，用 pg_ctl 作补充
    if pgctl_running is True:
        return ALIVE, evidence
    return UNKNOWN, evidence


def stop_postgres(pgdata: Path):
    """停止恢复实例（回收恢复进程）。失败残库必须不可再被查询。"""
    if not pgdata or not pgdata.exists():
        return
    try:
        run_cmd(
            ["gosu", "postgres", f"{PG_BIN}/pg_ctl", "-D", str(pgdata),
             "stop", "-m", "immediate", "-w", "-t", "30"],
            timeout=60,
        )
    except Exception as e:
        print(f"[recovery] stop_postgres error: {e}", flush=True)


def tail_file(path: Path, n=80, max_bytes=65536) -> str:
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - max_bytes))
            data = f.read().decode("utf-8", "replace")
        return "\n".join(data.splitlines()[-n:])
    except Exception:
        return ""


def fail(run: dict, msg: str, pgdata: Path | None = None):
    """进入 FAILED：先停止并回收恢复进程，残库不开放查询；重试需先回收残库。"""
    mgr.event("失败：" + msg)
    if pgdata is not None:
        stop_postgres(pgdata)
    with mgr.lock:
        if mgr.run is not run:
            return           # 任务已被回收/替换：不得覆盖新任务状态
        run["error"] = msg
        run["finished_at"] = now_iso()
        mgr.state = FAILED
        mgr.epoch += 1      # 版本递增：并发的汇总/明细请求不得再交付旧结果


def invalidate(run: dict, reason: str, evidence: list[str] | None = None):
    """已确认实例停止：当前任务失效，旧汇总/明细拒绝交付。副本保留待回收。"""
    with mgr.lock:
        if mgr.run is not run or mgr.state not in RESULT_STATES + (VERIFYING,):
            return
        mgr.state = INVALIDATED
        mgr.epoch += 1
        run["invalidated"] = True
        run["invalid_reason"] = reason
        run["probe_evidence"] = evidence or []
        run["invalidated_at"] = now_iso()
        run["finished_at"] = now_iso()
        run["error"] = reason
    mgr.event("认定失效：" + reason + (
        "（" + "；".join(evidence) + "）" if evidence else ""))


def suspend(run: dict, evidence: list[str] | None = None):
    """暂无法确认实例状态：暂停结果开放，保留副本与占用。"""
    with mgr.lock:
        if mgr.run is not run or mgr.state not in RESULT_STATES:
            return
        if mgr.state == SUSPENDED:
            run["probe_evidence"] = evidence or run.get("probe_evidence")
            return
        mgr.state = SUSPENDED
        mgr.epoch += 1
        run["suspended"] = True
        run["probe_evidence"] = evidence or []
        run["suspended_at"] = now_iso()
    mgr.event("结果暂停开放：" + UNKNOWN_REASON +
              ("（" + "；".join(evidence) + "）" if evidence else ""))


def resume(run: dict, evidence: list[str] | None = None):
    """确认实例仍存活且证据仍在：恢复结果开放。

    只有此前因“暂无法确认”而暂停的任务才回到 READY；已失效（INVALIDATED）
    或已回收的任务绝不能因一次探活成功被重新开放。
    """
    with mgr.lock:
        if mgr.run is not run or mgr.state != SUSPENDED or run.get("invalidated"):
            return
        mgr.state = READY
        mgr.epoch += 1
        run["suspended"] = False
        run["probe_evidence"] = evidence or ["实例存活，结果恢复开放"]


def revalidate(run: dict):
    """结果开放期间确认存活状态：停止->失效，无法确认->暂停，存活->保持/恢复。"""
    if run.get("reclaiming"):
        return None          # 回收进行中：结果门已关闭，不再做存活判定
    state, evidence = probe_instance(Path(run["rundir"]) / "pgdata")
    if state == STOPPED:
        invalidate(run, STOPPED_REASON, evidence)
    elif state == UNKNOWN:
        suspend(run, evidence)
    else:
        resume(run, evidence)
    return state


def do_recovery(run: dict):
    rundir = Path(run["rundir"])
    pgdata = rundir / "pgdata"
    logfile = rundir / "postgres.log"
    rp = run["restore_point"]
    try:
        # ---- 1. 复制物理基线备份 ----
        mgr.event(f"复制物理基线备份 {BASEBACKUP_DIR} -> {pgdata}")
        shutil.copytree(BASEBACKUP_DIR, pgdata, symlinks=True)

        # ---- 2. 写入恢复配置（WAL 归档重放 + 命名恢复点精确停止） ----
        missing = run.get("missing_segment") or ""
        restore_cmd = f"{RESTORE_SCRIPT} %f %p" + (f" {missing}" if missing else "")
        if missing:
            mgr.event(f"注入归档缺段故障：段 {missing} 将在 restore_command 中缺失")
        with open(pgdata / "postgresql.auto.conf", "a", encoding="utf-8") as f:
            f.write("\n# ---- recovery drill ----\n")
            f.write(f"port = {RECOVERY_PORT}\n")
            f.write("listen_addresses = '127.0.0.1'\n")
            f.write("unix_socket_directories = '/tmp'\n")
            f.write("archive_mode = off\n")
            f.write(f"restore_command = '{restore_cmd}'\n")
            f.write(f"recovery_target_name = '{rp}'\n")
            f.write("recovery_target_action = 'promote'\n")
            f.write("hot_standby = on\n")
            f.write("default_transaction_read_only = on\n")
        (pgdata / "recovery.signal").touch()
        mgr.event(f"恢复配置就绪：recovery_target_name='{rp}'，restore_command 指向 WAL 归档")

        run_cmd(["chown", "-R", "postgres:postgres", str(pgdata)])
        run_cmd(["chown", "postgres:postgres", str(rundir)])
        run_cmd(["chmod", "700", str(pgdata)])

        # ---- 3. 启动恢复实例，开始重放 WAL ----
        mgr.event("启动恢复实例，开始从 WAL 归档重放")
        with mgr.lock:
            mgr.state = RECOVERING
        r = run_cmd(
            ["gosu", "postgres", f"{PG_BIN}/pg_ctl", "-D", str(pgdata),
             "-l", str(logfile), "-w", "-t", "120", "start"],
            timeout=150,
        )
        if r.returncode != 0:
            # 恢复未完成即失败：停止实例并按“失败残库”保留，必须显式回收
            fail(run,
                 f"恢复实例未能进入服务状态（pg_ctl 退出码 {r.returncode}）。"
                 "通常为归档缺段或未到达目标恢复点，详见日志。",
                 pgdata)
            return
        poll_until_ready(run, pgdata, logfile, rp)
    except Exception as e:
        if pgdata.exists():
            fail(run, f"恢复过程异常：{e}", pgdata)
        else:
            fail(run, f"恢复过程异常：{e}")


# 轮询连接在恢复早期会被拒，这类连接级 FATAL 是良性的，不能据此判失败
BENIGN_FATALS = (
    "the database system is not yet accepting connections",
    "the database system is starting up",
    "the database system is in recovery mode",
)


def poll_until_ready(run: dict, pgdata: Path, logfile: Path, rp: str):
    deadline = time.time() + RECOVERY_TIMEOUT
    target_re = re.compile(r'recovery stopping at restore point "%s"' % re.escape(rp))
    while time.time() < deadline:
        log = tail_file(logfile, n=400, max_bytes=524288)
        if "recovery ended before configured recovery target was reached" in log:
            fail(run, "未到达目标恢复点：WAL 重放提前结束（归档缺段或目标不在归档中）。", pgdata)
            return
        # 非良性 FATAL 需进程确认退出后才判失败（避免连接期误报）
        fatals = [f for f in re.findall(r"FATAL:\s+(.+)", log)
                  if not any(b in f for b in BENIGN_FATALS)]
        if fatals:
            time.sleep(2)
            st = run_cmd([f"{PG_BIN}/pg_ctl", "-D", str(pgdata), "status"])
            if st.returncode != 0:
                fail(run, f"恢复实例报错：{fatals[-1].strip()}", pgdata)
                return
        # 热备模式下可在重放期间只读连接，用于展示进度；可连接绝不等于成功
        try:
            with psycopg.connect(recovered_dsn(), autocommit=True) as conn:
                in_rec, lsn, ts = conn.execute(
                    "SELECT pg_is_in_recovery(), pg_last_wal_replay_lsn(), "
                    "pg_last_xact_replay_timestamp()"
                ).fetchone()
            run["replay_lsn"] = str(lsn) if lsn is not None else None
            run["replay_ts"] = str(ts) if ts is not None else None
            if not in_rec:
                # 已 promote —— 必须再核对日志中的目标落点证据
                with mgr.lock:
                    if mgr.run is not run:
                        return       # 任务已被回收/替换
                    mgr.state = VERIFYING
                mgr.event("实例已提升（promote），核对目标恢复点落点证据…")
                log = tail_file(logfile, n=400, max_bytes=524288)
                if target_re.search(log) and "archive recovery complete" in log:
                    gather_results(run)
                    with mgr.lock:
                        if mgr.run is run and mgr.state == VERIFYING:
                            mgr.state = READY
                            mgr.epoch += 1
                            run["finished_at"] = now_iso()
                            mgr.event(f"已确认到达目标恢复点 {rp}，结果开放查询")
                    return
                fail(run, "实例已提升但日志缺少目标恢复点落点记录，拒绝开放结果。", pgdata)
                return
        except psycopg.OperationalError:
            pass
        except Exception as e:
            print(f"[recovery] poll error: {e}", flush=True)
        # 进程异常退出且未就绪 -> 失败
        st = run_cmd([f"{PG_BIN}/pg_ctl", "-D", str(pgdata), "status"])
        if st.returncode != 0:
            log = tail_file(logfile, n=400, max_bytes=524288)
            if "database system is ready to accept connections" not in log:
                fail(run, "恢复实例进程退出且未报告就绪。", pgdata)
                return
        time.sleep(1.5)
    fail(run, f"超过 {RECOVERY_TIMEOUT}s 仍未到达目标恢复点。", pgdata)


def read_result(run: dict) -> dict:
    """汇总读取：结果已开放后，先确认实例存活，再取缓存汇总。

    返回 dict 的 `state` 为最终状态（READY / SUSPENDED / 其它），
    调用方必须按该状态决定是否交付结果。整套读取期间用 mgr.lock 串行化，
    保证“存活复核 -> 读取缓存 -> 返回”之间不会被并发的状态查询插入旧结果。
    """
    if run.get("result") is None:
        raise HTTPException(409, "恢复库尚未完成结果校验，结果不开放查询")
    with mgr.lock:
        state = mgr.state
        epoch = mgr.epoch
    if state not in RESULT_STATES:
        raise HTTPException(409, _deny_reason(run, state))
    revalidate(run)
    with mgr.lock:
        state = mgr.state
        if state == READY and mgr.run is run and run.get("result") is not None:
            return {"state": READY, "result": run["result"], "epoch": mgr.epoch}
        evidence = list(run.get("probe_evidence") or [])
        epoch = mgr.epoch
    if state == SUSPENDED:
        return {"state": SUSPENDED, "result": None, "reason": UNKNOWN_REASON,
                "evidence": evidence, "epoch": epoch}
    raise HTTPException(409, _deny_reason(run, state))


def _deny_reason(run: dict, state: str) -> str:
    """结果被拒时的原因说明（附带状态查询已确认的失效/暂停证据）。"""
    if state == INVALIDATED or (run and run.get("invalidated")):
        reason = (run or {}).get("invalid_reason") or STOPPED_REASON
    elif state == SUSPENDED:
        reason = UNKNOWN_REASON
    elif state == IDLE:
        reason = "恢复任务已回收，结果不再提供"
    else:
        reason = "恢复库未确认到达目标恢复点，结果不开放查询"
    evidence = (run or {}).get("probe_evidence") or []
    if evidence:
        reason += "（判定依据：" + "；".join(evidence) + "）"
    return reason


def gather_results(run: dict):
    manifest = load_manifest()
    rpinfo = next(r for r in manifest["restore_points"] if r["name"] == run["restore_point"])
    ds = manifest["deleted_set"]
    with psycopg.connect(recovered_dsn(), autocommit=True) as conn:
        total = conn.execute("SELECT count(*) FROM telemetry_events").fetchone()[0]
        ds_rows = conn.execute(
            "SELECT count(*) FROM telemetry_events WHERE device_id=%s AND metric=%s",
            (ds["device_id"], ds["metric"]),
        ).fetchone()[0]
        mn, mx = conn.execute(
            "SELECT min(recorded_at), max(recorded_at) FROM telemetry_events"
        ).fetchone()
        # 与清单预期值在数据库内做精确比较，避免时区字符串格式差异造成误判
        mx_match = conn.execute(
            "SELECT max(recorded_at) IS NOT DISTINCT FROM %s::timestamptz "
            "FROM telemetry_events",
            (rpinfo["max_recorded_at"],),
        ).fetchone()[0]
        matrix = conn.execute(
            "SELECT device_id, metric, count(*) FROM telemetry_events "
            "GROUP BY 1,2 ORDER BY 1,2"
        ).fetchall()
    checks = [
        {"label": "遥测总行数", "actual": total,
         "expected": rpinfo["expected_total"], "ok": total == rpinfo["expected_total"]},
        {"label": f"误删集行数（{ds['device_id']} / {ds['metric']}）", "actual": ds_rows,
         "expected": rpinfo["deleted_set_rows"], "ok": ds_rows == rpinfo["deleted_set_rows"]},
        {"label": "最新 recorded_at", "actual": str(mx),
         "expected": rpinfo["max_recorded_at"], "ok": bool(mx_match)},
    ]
    run["result"] = {
        "restore_point": rpinfo["name"],
        "total": total,
        "deleted_set_rows": ds_rows,
        "min_recorded_at": str(mn),
        "max_recorded_at": str(mx),
        "matrix": [{"device_id": d, "metric": m, "count": c} for d, m, c in matrix],
        "expected": rpinfo,
        "checks": checks,
    }


# ---------------------------------------------------------------------------
# 应用生命周期：启动时清理上一轮遗留的恢复实例，保证“全站同时只恢复一份”
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    for d in sorted(RUNS_DIR.glob("run_*")):
        pgdata = d / "pgdata"
        if (pgdata / "postmaster.pid").exists():
            stop_postgres(pgdata)
        shutil.rmtree(d, ignore_errors=True)
    yield


app = FastAPI(title="遥测恢复演练台", lifespan=lifespan,
              docs_url=None, redoc_url=None, openapi_url=None)


class RecoverRequest(BaseModel):
    restore_point: str
    inject_fault: bool = False


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
def api_health():
    return {"ok": True}


@app.get("/api/manifest")
def api_manifest():
    return load_manifest()


@app.get("/api/primary/stats")
def api_primary_stats():
    """原库实时状态：证明演练期间原库持续入仓、不受影响。"""
    try:
        with psycopg.connect(PRIMARY_DSN, autocommit=True) as conn:
            total, latest = conn.execute(
                "SELECT count(*), max(recorded_at) FROM telemetry_events"
            ).fetchone()
            live = conn.execute(
                "SELECT count(*) FROM telemetry_events WHERE device_id='WX-LIVE'"
            ).fetchone()[0]
            arch = conn.execute(
                "SELECT last_archived_wal, last_archived_time, archived_count, failed_count "
                "FROM pg_stat_archiver"
            ).fetchone()
        return {
            "reachable": True,
            "total": total,
            "live_rows": live,
            "latest_recorded_at": str(latest),
            "archiver": {
                "last_archived_wal": arch[0],
                "last_archived_time": str(arch[1]),
                "archived_count": arch[2],
                "failed_count": arch[3],
            },
        }
    except Exception as e:
        return {"reachable": False, "error": str(e)}


@app.post("/api/recover")
def api_recover(req: RecoverRequest):
    manifest = load_manifest()
    rp = next((r for r in manifest["restore_points"] if r["name"] == req.restore_point), None)
    if rp is None:
        raise HTTPException(400, f"未知恢复点: {req.restore_point}")
    with mgr.lock:
        if mgr.state == IDLE and mgr.run is not None:
            # 理论上不会出现：残留任务必须显式回收，不能悄悄覆盖
            raise HTTPException(409, "存在未回收的恢复副本，请先回收")
        if mgr.state != IDLE:
            hint = {
                FAILED: "请先回收失败残库。",
                INVALIDATED: "请先回收已失效的副本及占用。",
                SUSPENDED: "实例状态未确认、结果暂停中：请先确认停止并回收副本。",
            }.get(mgr.state, "请先回收当前恢复库。")
            raise HTTPException(
                409, f"当前状态为 {mgr.state}：全站同时只允许一份恢复。" + hint)
    if not BASEBACKUP_DIR.exists():
        raise HTTPException(500, "物理基线备份不存在，无法恢复")
    with mgr.lock:
        if mgr.state != IDLE or mgr.run is not None:     # 复查，避免并发双启动
            raise HTTPException(409, "已有恢复任务在运行，全站同时只允许一份恢复")
        run_id = time.strftime("run_%Y%m%d_%H%M%S_") + req.restore_point
        rundir = RUNS_DIR / run_id
        rundir.mkdir(parents=True, exist_ok=False)
        mgr.epoch += 1          # 新任务：旧的结果版本一律作废
        mgr.run = {
            "id": run_id,
            "restore_point": req.restore_point,
            "inject_fault": req.inject_fault,
            "missing_segment": manifest.get("first_wal_after_backup") if req.inject_fault else None,
            "started_at": now_iso(),
            "finished_at": None,
            "rundir": str(rundir),
            "events": [],
            "replay_lsn": None,
            "replay_ts": None,
            "error": None,
            "result": None,
            "invalidated": False,
            "invalid_reason": None,
            "suspended": False,
            "reclaiming": False,
            "probe_evidence": [],
        }
        mgr.state = PREPARING
        mgr.event(
            f"创建恢复任务 {run_id}，目标恢复点 {req.restore_point}"
            + ("（注入归档缺段故障）" if req.inject_fault else "")
        )
    threading.Thread(target=do_recovery, args=(mgr.run,), daemon=True).start()
    return {"ok": True, "run_id": run_id}


@app.get("/api/status")
def api_status():
    """恢复状态。结果开放期间（READY/SUSPENDED）先复核实例存活：

    - 确认实例已停止 -> 任务失效（INVALIDATED），旧汇总/明细拒绝交付
    - 暂无法确认 -> 结果暂停开放（SUSPENDED），保留副本与占用
    - 确认存活 -> 保持/恢复 READY
    仅复核 READY/SUSPENDED（结果开放相关状态）；恢复进行中绝不打断。
    """
    with mgr.lock:
        state = mgr.state
        run_ = mgr.run
    if run_ is not None and state in RESULT_STATES:
        revalidate(run_)
    with mgr.lock:
        state = mgr.state
        run = dict(mgr.run) if mgr.run else None
    log_tail = ""
    if run:
        log_tail = tail_file(Path(run["rundir"]) / "postgres.log", 80)
    return {"state": state, "run": run, "log_tail": log_tail,
            "result_open": state == READY,
            "epoch": mgr.epoch}


def stop_and_verify(pgdata: Path) -> tuple[bool, list[str]]:
    """停止恢复实例并复核：只有确认进程已停止才允许删除副本/释放占用。"""
    stop_postgres(pgdata)
    state, evidence = probe_instance(pgdata)
    return state == STOPPED, evidence


@app.post("/api/drill/kill-instance")
def api_drill_kill_instance():
    """演练辅助：模拟恢复实例“意外退出”（向 postmaster 发 SIGQUIT 立即关闭）。

    仅用于本地演练，验证“结果开放与实例存活绑定”：下一次访问状态/汇总/明细
    时探测到实例已停止，当前任务立即失效，旧结果拒绝交付；随后可回收并重试。
    """
    with mgr.lock:
        if mgr.state not in RESULT_STATES:
            raise HTTPException(409, f"当前状态 {mgr.state}，仅结果开放时可模拟实例退出")
        run = mgr.run
        pgdata = Path(run["rundir"]) / "pgdata"
        pid = instance_pid(pgdata)
    if pid is None:
        raise HTTPException(409, "未找到恢复实例进程（postmaster.pid）")
    try:
        os.kill(pid, signal.SIGQUIT)     # PostgreSQL 立即关闭：连子进程一并终止
    except ProcessLookupError:
        pass
    except Exception as e:
        raise HTTPException(500, f"模拟退出失败：{e}")
    mgr.event("演练：已向恢复实例 postmaster 发送 SIGQUIT，模拟实例意外停止")
    # 等待进程真正消失，避免页面瞬间仍探测到存活
    deadline = time.time() + 15
    while time.time() < deadline and instance_process_alive(pgdata):
        time.sleep(0.2)
    return {"ok": True, "stopped_pid": pid}


@app.post("/api/reclaim")
def api_reclaim():
    """回收：确认停止恢复实例并删除副本。READY/FAILED/SUSPENDED/INVALIDATED 可回收，
    回到 IDLE 且版本号递增后才允许再次恢复。"""
    with mgr.lock:
        if mgr.state not in (READY, FAILED, SUSPENDED, INVALIDATED):
            raise HTTPException(409, f"当前状态 {mgr.state} 无需回收")
        run = mgr.run
        if run is None:
            raise HTTPException(409, "没有可回收的恢复副本")
        rundir = run["rundir"]
        # 回收开始即关闭结果门：汇总/明细不得在停止过程中继续交付
        mgr.state = SUSPENDED
        mgr.epoch += 1
        run["reclaiming"] = True
    confirmed, evidence = stop_and_verify(Path(rundir) / "pgdata")
    if not confirmed:
        with mgr.lock:
            if mgr.run is run:
                mgr.state = SUSPENDED
                run["suspended"] = True
                run["reclaiming"] = False
                run["probe_evidence"] = evidence
        raise HTTPException(
            409,
            "未能确认恢复实例已停止，拒绝删除副本：保留副本与占用。判定依据："
            + "；".join(evidence),
        )
    shutil.rmtree(rundir, ignore_errors=True)
    with mgr.lock:
        run["reclaiming"] = False
        if mgr.run is run:
            mgr.state = IDLE
            mgr.run = None
        mgr.epoch += 1
    mgr.event("已确认实例停止并回收副本，恢复占用释放（可重新发起恢复）")
    return {"ok": True, "state": "IDLE"}


@app.get("/api/recovered/summary")
def api_recovered_summary():
    """汇总：读取前复核实例存活；读取期间若被并发状态查询判定失效/暂停，
    即使缓存汇总已在手，也不得再交付旧结果。"""
    with mgr.lock:
        run = mgr.run
        state = mgr.state
    if run is None or state not in RESULT_STATES:
        raise HTTPException(409, _deny_reason(run, state))
    r = read_result(run)
    with mgr.lock:
        stale = mgr.epoch != r["epoch"] or mgr.state != READY or mgr.run is not run
        current_state = mgr.state
        current_run = mgr.run or run
    if r["state"] != READY:
        # 读取时的存活复核已判定失效/暂停：按该状态拒绝，并附原因
        raise HTTPException(409, _deny_reason(current_run, current_state))
    if stale:
        raise HTTPException(
            409,
            "结果在读取期间已失效或暂停，拒绝交付旧汇总。"
            + _deny_reason(current_run, current_state))
    return r["result"]


@app.get("/api/recovered/rows")
def api_recovered_rows(
    device_id: str | None = None,
    metric: str | None = None,
    limit: int = Query(50, ge=1, le=500),
):
    """明细：先确认实例存活，再查询。

    - 已确认停止 -> 任务失效，明细拒绝并说明原因
    - 暂无法确认 -> 结果暂停开放，保留副本与占用
    - 确认存活时，单次连接失败只报告查询错误，不误清恢复任务
    """
    with mgr.lock:
        run = mgr.run
        state = mgr.state
        epoch = mgr.epoch
    if run is None or state not in RESULT_STATES or run.get("result") is None:
        raise HTTPException(409, _deny_reason(run, state))
    st, evidence = probe_instance(Path(run["rundir"]) / "pgdata")
    if st == STOPPED:
        invalidate(run, STOPPED_REASON, evidence)
        raise HTTPException(409, _deny_reason(run, INVALIDATED))
    if st == UNKNOWN:
        suspend(run, evidence)
        raise HTTPException(409, _deny_reason(run, SUSPENDED))

    # 探测期间状态可能被并发的状态查询改动：确认仍在同一 READY 版本上
    with mgr.lock:
        if mgr.epoch != epoch or mgr.state != READY or mgr.run is not run:
            raise HTTPException(
                409,
                "结果在探测期间已失效或暂停，拒绝交付旧明细。"
                + _deny_reason(mgr.run or run, mgr.state))
        epoch = mgr.epoch      # 以探测后的版本作为查询结果的有效性基线

    for v in (device_id, metric):
        if v is not None and not re.fullmatch(r"[A-Za-z0-9_\-]{1,40}", v):
            raise HTTPException(400, "非法过滤参数")
    sql = ("SELECT id, device_id, site, metric, value, recorded_at, ingested_at "
           "FROM telemetry_events")
    conds, params = [], []
    if device_id:
        conds.append("device_id = %s")
        params.append(device_id)
    if metric:
        conds.append("metric = %s")
        params.append(metric)
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY recorded_at DESC, id DESC LIMIT %s"
    params.append(limit)
    try:
        with psycopg.connect(
            recovered_dsn("options='-c default_transaction_read_only=on'"),
            autocommit=True,
            row_factory=dict_row,
        ) as conn:
            rows = conn.execute(sql, params).fetchall()
    except Exception as e:
        # 连接失败不等于实例已停止：先复核进程；只有确认停止才使任务失效
        st2, evidence2 = probe_instance(Path(run["rundir"]) / "pgdata")
        if st2 == STOPPED:
            invalidate(run, STOPPED_REASON, evidence2)
            raise HTTPException(409, _deny_reason(run, INVALIDATED))
        if st2 == UNKNOWN:
            suspend(run, evidence2)
            raise HTTPException(409, _deny_reason(run, SUSPENDED))
        raise HTTPException(
            503,
            f"恢复库查询错误（未据此判定实例停止，恢复任务保持）：{e}",
        )
    with mgr.lock:
        if mgr.epoch != epoch or mgr.state != READY or mgr.run is not run:
            stale_state = mgr.state
            stale_run = mgr.run or run
            raise HTTPException(
                409,
                "结果在查询期间已失效或暂停，拒绝交付旧明细。"
                + _deny_reason(stale_run, stale_state))
    for r in rows:
        r["recorded_at"] = str(r["recorded_at"])
        r["ingested_at"] = str(r["ingested_at"])
    return {"rows": rows}
