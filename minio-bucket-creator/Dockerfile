# Use an official lightweight Python image
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy the Pipfile and Pipfile.lock
COPY Pipfile Pipfile.lock ./

# Install dependencies using Pipenv
RUN pip install --no-cache-dir pipenv
RUN pipenv install --system --deploy

# Create a non-root user
RUN useradd -m appuser && chown -R appuser:appuser /app

# Switch to the non-root user
USER appuser

# Copy the script into the container
COPY create_bucket.py .

# Define the entry point for the container
ENTRYPOINT ["python", "create_bucket.py"]
