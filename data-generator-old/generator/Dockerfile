FROM python:3.11-slim

WORKDIR /app

# Install dependencies dulu (cache layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy script generator
COPY generate.py .

CMD ["python", "-u", "generate.py"]
