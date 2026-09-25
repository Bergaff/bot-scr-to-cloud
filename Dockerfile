# Образ радара для Cloudflare Containers.
#
# Cloudflare собирает и запускает только linux/amd64. Если собираешь образ локально
# на машине с ARM (Apple Silicon, Raspberry Pi), добавь --platform:
#     docker build --platform linux/amd64 -t radar .
# В Workers Builds образ собирается сам, платформа там правильная.
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
	PYTHONDONTWRITEBYTECODE=1 \
	PIP_NO_CACHE_DIR=1 \
	PIP_DISABLE_PIP_VERSION_CHECK=1 \
	TZ=UTC

WORKDIR /app

# ca-certificates нужен для HTTPS до Telegram и до R2; больше ничего системного не требуется:
# sqlite3 встроен в Python, psutil мы сознательно не ставим (метрики берутся из /proc).
RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates \
	&& rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# .dockerignore не пускает в образ .env, *.session, *.sqlite3 и прочее локальное состояние:
# секреты приезжают в контейнер переменными окружения от Worker'а, а состояние — из R2.
COPY . .

# Быстрая проверка образа: всё импортируется и офлайн-тесты проходят ещё до деплоя.
RUN python -c "import monitor, bot_panel, metrics, forwarder, matcher, core_telegram; print('[i] импорт радара ok')" \
	&& python -c "import telethon, yaml, qrcode, socks; print('[i] зависимости ok:', telethon.__version__)" \
	&& python deploy/selftest_cloud.py >/dev/null \
	&& python selftest_metrics.py >/dev/null \
	&& echo '[i] самопроверки ok'

# Порт, который ждёт Worker (defaultPort в src/index.js)
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
	CMD python -c "import sys,urllib.request; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=4).status in (200,204) else 1)"

CMD ["python", "-u", "deploy/cloud_entry.py"]
