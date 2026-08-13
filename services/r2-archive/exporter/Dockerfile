FROM docker.io/library/python:3.14.5-slim-trixie

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY services/r2-archive/exporter/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY services/r2-archive/exporter/ ./

CMD ["python", "run.py"]
