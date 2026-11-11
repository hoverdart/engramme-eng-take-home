FROM python:3.12-slim
WORKDIR /kit
COPY memkit ./memkit
ENV MEMKIT_PORT=7070 PYTHONUNBUFFERED=1
EXPOSE 7070
CMD ["python", "-m", "memkit.sidecar"]
