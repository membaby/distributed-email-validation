FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install --yes --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY ./ ./

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5005

CMD ["python", "-m", "src.main"]
