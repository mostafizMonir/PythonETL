FROM python:3.9-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Set work directory
WORKDIR /app

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash etl_user \
    && chown -R etl_user:etl_user /app
USER etl_user

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "from database_manager import DatabaseManager; from config import Config; \
    db = DatabaseManager(Config.get_source_connection_string()); \
    exit(0 if db.test_connection() else 1)"

# Default command
CMD ["python", "main.py"] 