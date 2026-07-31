FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install shared library
COPY shared/py /tmp/shared-py
RUN pip install --no-cache-dir /tmp/shared-py && rm -rf /tmp/shared-py

# Install service-specific dependencies
COPY services/polymarket/polymarket-active-markets/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy service code
COPY services/polymarket/polymarket-active-markets/ ./

CMD ["python", "run.py"]
