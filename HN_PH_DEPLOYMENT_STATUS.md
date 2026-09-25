# Hacker News + Product Hunt deployment

- Railway project: `cheerful-vibrancy`
- Service: `kiosco2-distribution-bot`
- Environment: `production`
- Deployment: `358e4607-a5c3-4c33-ae8d-704f62c9cd36`
- Image digest: `sha256:a1cd59ca80ca14725b49fe13656b4278ce6cda2a1268a43578fd07a26715a798`
- Public URL: `https://kiosco2-distribution-bot-production.up.railway.app`
- Persistent volume: `/app/data`
- LLM mode: `auto` → `groq/openai/gpt-oss-20b` with deterministic fallback

Verified runtime events:

```text
SQLite database initialized at /app/data/bot.db
Telegram API verified as @B2BLeadExtractor247Bot; startup notification sent
Dual monitor started; evaluator=groq/openai/gpt-oss-20b fallback=True
Product Hunt scan complete: fetched=50 new=0
Hacker News scan complete: fetched=132 new=0
GET /health -> 200
POST /api/v1/log-event (without API key) -> 401
POST /api/v1/log-event (with API key) -> 201
```

No Reddit credentials are required by the running image. Initial historical
items are deduplicated in SQLite. New qualified opportunities are sent to the
authorized Telegram chat with approval and discard buttons.

## Reconciliación

El coordinador desplegado quedó unificado: conserva HN y Product Hunt como
fuentes, activa Groq automáticamente cuando existe `GROQ_API_KEY`, comparte
`/app/data/bot.db` con las métricas dinámicas y atiende `/stats` desde el mismo
polling de Telegram. `METRICS_API_KEY` está configurada en Railway; las pruebas
autorizadas se registraron con timestamps históricos para no alterar el reporte
financiero del día.

Validación local: 9 pruebas superadas y todos los módulos compilaron sin error.
