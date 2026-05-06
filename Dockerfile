# Base image with Python
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy all project files
COPY . .

# Install dependencies
RUN pip install --no-cache-dir \
    requests \
    kafka-python \
    pyspark \
    pandas \
    flask

# Create required folders
RUN mkdir -p logs data/raw data/processed

# Default command — start the API
CMD ["python", "api/pipeline_api.py"]