FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TIKTOKEN_CACHE_DIR=/opt/tiktoken

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
 && python -c "import tiktoken; tiktoken.get_encoding('cl100k_base')"

COPY common ./common
COPY node_a ./node_a
COPY node_b ./node_b
COPY ui ./ui
COPY bench/results.csv ./bench/results.csv

RUN useradd --system --uid 10001 pagerkv && mkdir /data && chown pagerkv /data
USER pagerkv
VOLUME ["/data"]

# docker-compose.yml overrides this per service.
CMD ["uvicorn", "node_a.app:app", "--host", "0.0.0.0", "--port", "8001"]
