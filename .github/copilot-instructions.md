# Copilot Instructions for Appilot Backend API

These guidelines help AI coding agents work effectively in this FastAPI + MongoDB + Redis backend.

## Architecture & Key Modules
- **Entry point**: `main.py` creates the FastAPI app, mounts routers from `routes/`, configures logging (`logger.py`), Redis (`redis_client.py`), scheduler (`scheduler.py`), and WebSockets.
- **Database access**: `config/database.py` exposes the MongoDB client/DB; models in `models/` (`users.py`, `devices.py`, `tasks.py`, `bots.py`) define Pydantic models and collection handles (e.g. `tasks_collection`, `bots_collection`). Prefer reusing these collections instead of creating new clients.
- **Routing layer**: Individual route modules in `routes/` (e.g. `login.py`, `deviceRegistration.py`, `tasks.py`, `bots.py`, `twofa.py`, `passwordReset.py`) register routers that are mounted by `routes/routes.py` or directly in `main.py`. Follow existing patterns for new endpoints.
- **Task & device orchestration**:
  - `routes/tasks.py` handles CRUD and scheduling metadata for tasks (fields like `status`, `isScheduled`, `activeJobs`, `deviceIds`, `serverId`, `channelId`).
  - `routes/deviceRegistration.py` manages device registration and `/send_command` logic, including weekly planners, per-account caps, and Discord summaries.
  - `utils/weekly_scheduler.py` and `scheduler.py` integrate APScheduler for periodic jobs.
- **Auth & users**: JWT-based auth and user management live primarily in `routes/login.py`, `routes/twofa.py`, `routes/passwordReset.py`, and `Schemas/schema.py` for shared request/response models.
- **Realtime & external services**:
  - WebSockets and connection tracking use `connection_registry.py`, `redis_client.py`, and routes under `routes/deviceRegistration.py`.
  - Discord bot integration is implemented in `Bot/discord_bot.py` (notifications for schedules, errors, completion).

## Conventions & Patterns
- **Data models**: Use Pydantic models in `models/` for schema and validation. Collections are defined once per model file (e.g. `tasks_collection = db['tasks']`) and reused across routes via imports.
- **Request/response models**: Route-specific request bodies are defined with Pydantic `BaseModel` inside the corresponding `routes/*.py` file (e.g. `CommandRequest`, `StopTaskCommandRequest` in `routes/deviceRegistration.py`, multiple `*Request` models in `routes/tasks.py`). When adding endpoints, define minimal, explicit request models near the route.
- **Authentication**: Most protected routes depend on `get_current_user` from `utils/utils.py` via `Depends(get_current_user)`. New authenticated endpoints should follow the same pattern.
- **Status & state fields**:
  - Tasks use `status` values such as `"awaiting"`, `"scheduled"`, `"running"` and `isScheduled: bool`. Preserve and reuse these values when changing logic.
  - Devices use `status: bool` and `activationDate: str` as defined in `routes/deviceRegistration.py` and the README’s schema section.
- **Scheduling semantics**: Task scheduling types (`ExactStartTime`, `DurationWithTimeWindow`, `EveryDayAutomaticRun`) and weekly plan generation are implemented in `routes/deviceRegistration.py` and `utils/weekly_scheduler.py`. When modifying scheduling, respect these named types and existing weekly plan generation helpers.
- **Redis usage**: Use the shared client from `redis_client.py`. Common patterns: JSON-encode values, small helper wrappers like `cache_get_json` in `routes/tasks.py`, and Redis keys that are namespaced per user or task.
- **Logging**: Use the shared `logger` from `logger.py` (import as in existing routes) with context-rich messages, especially around scheduling, WebSocket communication, and command dispatch (see `/send_command` in `routes/deviceRegistration.py`).

## Workflows & Commands
- **Local development**:
  - Create and activate a virtualenv, install deps from `requirements.txt`.
  - Run the app directly for dev: `python main.py`.
  - For production-like runs use Gunicorn: `gunicorn -c gunicorn_conf.py main:app`.
- **Environment configuration**:
  - `.env` in the repo root provides `db_uri`, `REDIS_URL`, `JWT_SECRET_KEY`, Discord and email settings, and `WORKER_ID` for multi-worker setups (see README’s "Environment Configuration").
- **Testing**:
  - Tests (if present) are expected under `tests/`; run with `python -m pytest tests/` (see README). Prefer writing tests that hit FastAPI routes via a test client and cover Mongo/Redis interactions behind the existing abstractions.
- **Deployment**:
  - `deploy.sh` documents the production deployment pattern: pull latest code, restart `fastapi.service`, and reload nginx; Docker deployment uses `gunicorn_conf.py` and `CMD ["gunicorn", "-c", "gunicorn_conf.py", "main:app"]`.

## How to Extend Safely
- **Adding a new API route**:
  - Put it in the appropriate `routes/*.py` module or create a new module and register it from `routes/routes.py`/`main.py` using FastAPI’s `APIRouter`.
  - Use Pydantic models for request bodies and responses; enforce auth with `Depends(get_current_user)` where required.
  - Interact with MongoDB through the existing collections in `models/*` instead of raw clients.
- **Changing command/scheduling logic**:
  - Touch `/send_command` and related helpers only with care; they orchestrate:
    - Weekly/automatic schedule generation
    - Per-account caps and method mappings
    - Discord summary messages (chunked via helpers like `chunk_text_for_discord`).
  - Keep existing flags (`testMode`, `noTwoHighRule`, off/rest days, etc.) backwards compatible.
- **Working with WebSockets/devices**:
  - Follow the existing connection flow in `routes/deviceRegistration.py` and `connection_registry.py` (registration, status changes, heartbeat messages). Do not invent new message types without updating both backend and Android client expectations.

## Style & Quality
- Follow PEP 8 and use Black-style formatting (see README "Code Style").
- Use explicit type hints for new functions and Pydantic models.
- Prefer async FastAPI endpoints (`async def`) and offload blocking DB operations to `asyncio.to_thread` where the codebase already does so.
- Reuse helper functions (e.g. caching, ID generation, Discord notification utilities) from existing modules rather than duplicating logic.
