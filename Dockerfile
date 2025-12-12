# Dockerfile
FROM python:3.10-slim

# Create non-root user for security
RUN groupadd -r airflow && useradd -r -g airflow airflow

WORKDIR /opt/airflow

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    postgresql-client \
    build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY --chown=airflow:airflow . .

# Create necessary directories with proper permissions
RUN mkdir -p logs data reports \
    && chown -R airflow:airflow /opt/airflow

# Switch to non-root user
USER airflow

# Expose port for Airflow webserver
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

CMD ["bash", "-c", "airflow db init && airflow users create --username ${AIRFLOW_ADMIN_USER:-admin} --firstname Admin --lastname User --role Admin --email ${AIRFLOW_ADMIN_EMAIL:-admin@example.com} --password ${AIRFLOW_ADMIN_PASSWORD:-admin} && airflow webserver"]