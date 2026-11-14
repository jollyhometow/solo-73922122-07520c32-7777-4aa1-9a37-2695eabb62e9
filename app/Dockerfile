FROM postgres:16

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/app
COPY requirements.txt .
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt

COPY main.py .
COPY restore_command.sh /opt/app/restore_command.sh
COPY static/ /opt/app/static/
RUN chmod 755 /opt/app/restore_command.sh

EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=20 \
  CMD python3 -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/api/health', timeout=2).status==200 else 1)"
CMD ["python3", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
