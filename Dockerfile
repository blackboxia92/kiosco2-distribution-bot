FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN useradd --create-home --uid 10001 appuser

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY auto_distributor.py metrics.py hn_monitor.py ph_monitor.py source_item.py ./

RUN mkdir -p /app/data && chown -R appuser:appuser /app
USER appuser

CMD ["python", "auto_distributor.py"]
