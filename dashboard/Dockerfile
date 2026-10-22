
# syntax=docker/dockerfile:1

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt /tmp/requirements.txt

RUN python -m pip install \
    --no-cache-dir \
    --index-url https://pypi.org/simple \
    --timeout=600 \
    --retries=5 \
    -r /tmp/requirements.txt

COPY app.py /app/app.py

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501", "--browser.gatherUsageStats=false"]
