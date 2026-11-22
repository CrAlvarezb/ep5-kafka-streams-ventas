# API REST de usuarios (Flask) servida con gunicorn en el puerto 8081.
FROM python:3.12-slim
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
RUN useradd --no-create-home --uid 10001 api
USER api
EXPOSE 8081
# 1 proceso: los usuarios viven en memoria; con varios workers cada uno tendria su propia lista.
CMD ["gunicorn", "--bind", "0.0.0.0:8081", "--workers", "1", "--threads", "4", "--access-logfile", "-", "app:app"]
