FROM python:3.11-slim

WORKDIR /app

# System dependencies for Playright (Scrapling)
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget curl gnupg ca-certificates \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libxkbcommon0 \
    libxcomposite1 libxdamage1 libxrandr2 libgbm1 libasound2 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install browsers for Playwright and Scrapling
RUN python -m playwright install chromium --with-deps 2>/dev/null || true

COPY . .

RUN mkdir -p logs sessions

CMD ["python", "main.py"]
