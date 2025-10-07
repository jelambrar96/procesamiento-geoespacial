# Base image
FROM python:3.12-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Create and set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .

RUN apt-get update && apt-get install -y --no-install-recommends build-essential gettext-base
RUN pip install --no-cache-dir -r requirements.txt
RUN apt clean && rm -rf /var/lib/apt/lists/*

COPY . .

# Create a non-root user
RUN useradd -m appuser && chown -R appuser:appuser /app

# Switch to the non-root user
USER appuser

CMD ["/bin/bash", "run.sh"]
