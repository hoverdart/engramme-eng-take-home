FROM python:3.12-slim
WORKDIR /app
COPY starter-kit/memkit /app/starter-kit/memkit
COPY service /app/service
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 DATABASE_PATH=/data/memory.sqlite3
EXPOSE 8080
CMD ["python", "-m", "service.app"]
