# Dockerfile
FROM python:3.11-slim

# Install system dependencies for PDF and fonts
RUN apt-get update && apt-get install -y \
    libfreetype6-dev libjpeg-dev build-essential gcc && \
    apt-get clean

WORKDIR /app
COPY . /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose port
ENV PORT=8080
CMD ["python", "main.py"]
