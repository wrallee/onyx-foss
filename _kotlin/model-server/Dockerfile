FROM python:3.13-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HF_HUB_OFFLINE=1 \
    TRANSFORMERS_OFFLINE=1 \
    HOME=/tmp

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu torch==2.9.1 \
    && pip install --no-cache-dir -r requirements.txt

COPY app ./app

RUN useradd --system --uid 10001 --home-dir /tmp modelserver \
    && chown -R modelserver:modelserver /app /tmp
USER 10001

EXPOSE 9000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9000", "--workers", "1"]
