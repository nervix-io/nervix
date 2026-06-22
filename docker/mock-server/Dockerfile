FROM python:3.13-alpine

WORKDIR /app

RUN pip install --no-cache-dir aiohttp==3.12.15

COPY docker/mock-server/app.py /app/app.py

CMD ["python", "/app/app.py"]
