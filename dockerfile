FROM python:3.11-slim

# System deps (reportlab/fonts/Pillow often need these)
RUN apt-get update && apt-get install -y \
    libfreetype6-dev libjpeg-dev build-essential gcc \
  && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

# Optional: pin env so code can find the template explicitly
ENV N1_TEMPLATE_PATH=templates/N1.pdf

RUN pip install --no-cache-dir -r requirements.txt

ENV PORT=8080
CMD ["python", "main.py"]
