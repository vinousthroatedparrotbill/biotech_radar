# Biotech Radar — React(Vite) + FastAPI 단일 서비스 (Render Docker 배포)
# 1) Node로 React 빌드 → 2) Python에서 FastAPI가 /api/* + 빌드된 정적파일을 함께 서빙.

# ── 1) 프론트엔드 빌드 ──
FROM node:20-slim AS web
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

# ── 2) Python 런타임 ──
FROM python:3.13-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# 시스템 빌드 의존성(일부 wheel 폴백 대비) — 최소만.
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc libpq5 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install -r requirements.txt
# 챗봇 원문 크롤링(JS 렌더 페이지) fallback용 — Playwright chromium + 시스템 의존성.
# 설치 실패해도 배포는 진행(fetch_url이 web_fetch로 우아하게 폴백). '||true'로 빌드 비차단.
RUN python -m playwright install --with-deps chromium || echo "playwright chromium install 실패 — web_fetch 폴백 사용"

# 앱 코드 (.dockerignore가 .venv/web/node_modules/로그/.env 등 제외)
COPY . .
# 위 COPY에서 빠진 빌드 산출물만 별도 복사 (.dockerignore로 web/dist 제외했으므로)
COPY --from=web /web/dist ./web/dist

EXPOSE 8000
# Render가 주입하는 $PORT에 바인딩 (없으면 8000)
CMD ["sh", "-c", "uvicorn api:app --host 0.0.0.0 --port ${PORT:-8000}"]
