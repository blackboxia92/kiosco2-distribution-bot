# kiosco2-distribution-bot

Monitor de oportunidades para Hacker News y Product Hunt, con deduplicación en
SQLite y aprobación Human-in-the-Loop por Telegram.

## Fuentes

- Hacker News mediante la API pública de Algolia `search_by_date`. Se consultan
  `launch SaaS`, `launching`, `first 100 users`, `backlinks`, `directory` y
  `SaaS marketing`; después se aplica el filtro local de keywords requerido.
- Product Hunt mediante el feed Atom público `https://www.producthunt.com/feed`.
  Se extraen nombre, descripción, URL e instante de publicación.

No necesita credenciales de Reddit ni Product Hunt. El worker no publica
comentarios en esos sitios. Cuando se pulsa **🟢 Aprobar y Publicar**, marca la
oportunidad como aprobada y publica el borrador final dentro del chat de
revisión de Telegram, listo para uso manual. **🔴 Descartar** cierra la
oportunidad sin publicar el borrador.

## Evaluación

`LLM_PROVIDER=auto` selecciona Groq cuando existe `GROQ_API_KEY`, OpenAI cuando
existe `OPENAI_API_KEY` y, temporalmente, reglas deterministas cuando no hay
ninguna clave. El fallback se identifica como `rules/deterministic-v1` en logs
y Telegram; al añadir una clave no hace falta redesplegar código.

## Configuración

Copiar `.env.example` a `.env` y completar Telegram. Las variables principales
son:

```text
LLM_PROVIDER=auto
GROQ_API_KEY=
GROQ_MODEL=openai/gpt-oss-20b
OPENAI_API_KEY=
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=1577307373
CHECKOUT_URL=https://kiosco2-directory-submitter-production.up.railway.app/jobs
METRICS_API_KEY=<secreto-largo-y-aleatorio>
DATABASE_PATH=/app/data/bot.db
MIN_SCORE=8
MAX_APPROVALS_PER_DAY=5
HN_POLL_SECONDS=120
PH_POLL_SECONDS=300
```

## Ejecución local

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python auto_distributor.py --once
```

Servicio continuo:

```bash
python auto_distributor.py
```

## Docker

```bash
docker build -t kiosco2-distribution-bot .
docker run --rm --env-file .env \
  -v "$(pwd)/data:/app/data" \
  kiosco2-distribution-bot
```

## Railway

1. Vincular el servicio `kiosco2-distribution-bot`.
2. Montar un volumen en `/app/data`.
3. Cargar las variables de `.env.example`.
4. Ejecutar `railway up --detach -y`.
5. Verificar en logs:

```text
SQLite database initialized
Telegram API verified and startup notification sent
Dual monitor started
Hacker News scan complete: fetched=... new=...
Product Hunt scan complete: fetched=... new=...
```

Al arrancar se envía un mensaje automático al chat autorizado indicando las
fuentes y el proveedor de evaluación activo. En modo `auto`, si Groq/OpenAI
responde con un error transitorio o de modelo, el ítem se procesa con reglas y
el evaluador queda registrado como `rules/deterministic-v1 (fallback)`.

## Métricas y finanzas dinámicas

El mismo proceso expone una API FastAPI en el puerto indicado por `PORT`. Los
proyectos no están declarados en código: el primer evento crea el proyecto y
los eventos posteriores se agregan por `project_id`.

```bash
curl -X POST "https://<dominio>/api/v1/log-event" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $METRICS_API_KEY" \
  -d '{
    "project_id": "mi-micro-saas",
    "project_name": "Mi Micro SaaS",
    "event_type": "REVENUE",
    "amount": 49.90,
    "source": "stripe"
  }'
```

`event_type` acepta `REVENUE` o `COST`; `timestamp` es opcional y, cuando se
envía, debe incluir zona horaria. También se acepta
`Authorization: Bearer <METRICS_API_KEY>`.

El comando `/stats` solo responde en `TELEGRAM_CHAT_ID`. Resume la actividad
del día en ART por proyecto y muestra ingresos, costos y ganancia neta, además
del consolidado. El mismo informe se envía automáticamente todos los días a
las 20:00 ART. Las tablas `projects`, `financial_events` y
`operational_events` viven junto al resto del estado en `/app/data/bot.db`.
