FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# 53/udp  -> DNS forwarder (default command)
# 5353/udp -> sample upstream DNS (overridden command)
# 8000/tcp -> sample upstream FastAPI admin API
EXPOSE 53/udp 5353/udp 8000/tcp

# Default container runs the DNS forwarder; compose overrides for the upstream.
CMD ["python", "-m", "app.forwarder"]
