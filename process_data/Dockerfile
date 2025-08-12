FROM python:3.11-slim


RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libpq-dev \
    postgresql-client && \ 
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN  pip install --no-cache-dir -r requirements.txt
COPY overpass.py .
COPY import_csv.sql .
COPY process_data.sql .
COPY run.sh .
RUN  chmod +x run.sh
CMD  ./run.sh
