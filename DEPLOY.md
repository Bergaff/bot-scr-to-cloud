# Деплой радара в Cloudflare: Worker + контейнер (схема B)

Здесь — что нажимать и что вводить, чтобы радар заработал в облаке. Всё уже лежит в репозитории:
`Dockerfile`, `wrangler.jsonc`, `src/index.js` (Worker), `deploy/cloud_entry.py` (вход контейнера)
и `deploy/r2_state.py` (состояние в R2). Переписывать радар не пришлось: в контейнере работает
тот же `monitor.py`, только запускается по расписанию.

---

## Как это устроено (одна картинка словами)

```
Cloudflare cron (каждые 10 минут)
   └─> Worker (src/index.js)
         ├─ поднять/разбудить контейнер и дождаться порта 8080
         └─ POST http://localhost/run
               └─> контейнер (deploy/cloud_entry.py)
                     ├─ достать из R2: .session, hits.sqlite3, metrics.csv
                     ├─ python monitor.py --once --catchup 0 --notify bot --mode B
                     ├─ положить обратно в R2: базу, сессии, metrics.csv, лог
                     └─ ответить итогом прохода (JSON)
         └─ записать итог в лог Worker'а; контейнер засыпает через 15 минут простоя
```

Почему два слоя: Workers исполняют JavaScript/TypeScript/WASM (Python там урезан до стандартной
библиотеки, Telethon не ставится), поэтому Python живёт в контейнере — а контейнером управляет
Worker. Почему R2: диск контейнера **эфемерен**, после сна файловая система чистая, поэтому
`.session` и база хранятся в бакете.

---

## Шаг 0. Что нужно до начала

| Нужно | Зачем |
|---|---|
| Workers Paid ($5/мес) | Контейнеры доступны только на платном плане |
| Node.js 18+ на своей машине | для `npx wrangler` (секреты, загрузка сессии, логи) |
| Docker локально | **только** если деплоишь вручную; Workers Builds собирает образ сам |
| `TG_API_ID`, `TG_API_HASH` | как и раньше: https://my.telegram.org → API development tools |
| Бот и свой chat_id | `@BotFather` и `@userinfobot` (см. шаг 8 в `START-HERE.md`) |

Проверь, что Worker в дашборде называется **`bot-scr-to-cloud`** — ровно так стоит `name`
в `wrangler.jsonc`. Если назвал иначе, поправь одну строку в `wrangler.jsonc`, иначе деплой
создаст второго Worker'а.

---

## Шаг 1. Бакет R2 для состояния (2 минуты)

1. Дашборд → **R2 Object Storage** → **Create bucket** → имя `radar-state`
   (должно совпадать с `R2_BUCKET` в `wrangler.jsonc`).
2. **R2** → **Manage R2 API Tokens** → **Create API Token**:
   * Permissions: **Object Read & Write**;
   * Apply to: **specific bucket only** → `radar-state` (меньше прав — меньше риск);
   * TTL: можно оставить навсегда.
3. Сохрани **Access Key ID** и **Secret Access Key** — секрет показывается один раз.
4. **Account ID** виден в дашборде справа внизу (или в URL: `/accounts/<он>/...`).
5. Endpoint собирать не нужно: клиент сам склеит `https://<AccountID>.r2.cloudflarestorage.com`.

---

## Шаг 2. Секреты Worker'а (3 минуты)

Секреты в git не пишем — только в Cloudflare. Из папки проекта:

```bash
npx wrangler login

npx wrangler secret put TG_API_ID            # цифры из my.telegram.org
npx wrangler secret put TG_API_HASH          # длинная строка оттуда же
npx wrangler secret put TG_BOT_TOKEN         # токен бота от @BotFather
npx wrangler secret put TG_NOTIFY_CHAT       # твой числовой id от @userinfobot
npx wrangler secret put R2_ACCOUNT_ID        # из шага 1
npx wrangler secret put R2_ACCESS_KEY_ID     # из шага 1
npx wrangler secret put R2_SECRET_ACCESS_KEY # из шага 1
npx wrangler secret put RADAR_TOKEN          # придумай сам, ТОЛЬКО ASCII
```

`RADAR_TOKEN` — пароль к проверочным адресам Worker'а. Сгенерировать:

```bash
python3 -c "import secrets; print(secrets.token_urlsafe(24))"
```

Кириллицу в токен не ставь: Worker передаёт его HTTP-заголовком, а заголовки живут в latin-1
(контейнер при старте честно предупредит, если токен окажется не-ASCII).

То же самое можно ввести в дашборде: **Workers & Pages → bot-scr-to-cloud → Settings →
Variables & Secrets** (тип **Secret**).

Несекретное уже прописано в `wrangler.jsonc` → `vars`: `R2_BUCKET`, `TG_SESSION`,
`RADAR_ARGS` (аргументы прохода) и `RADAR_TIMEOUT` (секунды на проход).

Необязательные переменные (трогать не нужно, значения по умолчанию рабочие):

| Переменная | По умолчанию | Зачем |
|---|---|---|
| `RADAR_PORT` | `8080` | порт внутри контейнера; должен совпадать с `defaultPort` в `src/index.js` |
| `RADAR_WORKDIR` | каталог приложения | где проходит проход: там живут `.session`, база, `metrics.csv` |
| `RADAR_PYTHON` | `python3` (`sys.executable`) | чем запускать `monitor.py` |
| `RADAR_DB` | `hits.sqlite3` | имя файла базы в рабочем каталоге |
| `RADAR_METRICS_CSV` | `metrics.csv` | имя файла срезов расхода |
| `RADAR_SESSIONS` | `TG_SESSION` | несколько сессий через запятую, если аккаунтов больше одного |
| `R2_ENDPOINT` | `https://<R2_ACCOUNT_ID>.r2.cloudflarestorage.com` | переопределить, только если endpoint нестандартный |

---

## Шаг 3. Загрузить сессию в R2 (один раз, 2 минуты)

Контейнер не умеет вводить телефон и код из Telegram — входа там нет. Поэтому сессию надо
сделать на своей машине и положить в бакет:

```bat
:: Windows: войти по QR (или коду) — как обычно
start.bat --login-qr
```

```bash
# затем отправить файл сессии в R2 (имя = TG_SESSION, по умолчанию monitor_session)
npx wrangler r2 object put radar-state/sessions/monitor_session.session ^
  --file monitor_session.session --remote
```

Проверить, что файл на месте:

```bash
npx wrangler r2 object get radar-state/sessions/monitor_session.session --file /tmp/check.session --remote
```

`.session` — это **полный доступ к твоему аккаунту Telegram**. В git он не попадает
(`.gitignore`), в образ контейнера тоже (`.dockerignore`) — только в R2.

Если аккаунтов несколько, положи каждый: `sessions/<имя>.session`, и перечисли имена в
переменной `RADAR_SESSIONS` (через запятую) либо в `accounts:` в `sources.yaml`.

---

## Шаг 4. Production-ветка в настройках сборки (1 минута)

Дашборд → **Workers & Pages → bot-scr-to-cloud → Settings → Builds**:

| Настройка | Значение |
|---|---|
| Git repository | `Bergaff/bot-scr-to-cloud` |
| Production branch | `arena/01a0d4d8-bot-scr-to-cloud` (после слияния PR — `main`) |
| Build command | `npm install` |
| Deploy command | `npx wrangler deploy` |
| Root directory | `/` |

**Это важно.** Для НЕ-production веток Workers Builds по умолчанию выполняет
`wrangler versions upload`, который загружает только код Worker'а и **не собирает образ и не
раскатывает контейнер**. То есть push в обычную ветку контейнер не обновит — production-ветка
должна быть указана явно.

---

## Шаг 5. Первый деплой

Дальше всё делает push в production-ветку: Workers Builds ставит зависимости, собирает
Dockerfile, публикует образ и раскатывает Worker'а.

```bash
git push origin arena/01a0d4d8-bot-scr-to-cloud
```

Сборку видно в **Settings → Builds** (и статусом в GitHub: проверка «Workers Builds»).
Первый деплой прогревается несколько минут: URL Worker'а может отвечать раньше, чем контейнер
готов, — это нормально.

Вручную, с Docker на машине:

```bash
npm install
npx wrangler deploy
```

---

## Шаг 6. Проверка (2 минуты)

URL Worker'а: `https://bot-scr-to-cloud.<твой-субдомен>.workers.dev`
(субдомен виден в дашборде на странице Worker'а).

```bash
URL="https://bot-scr-to-cloud.<твой-субдомен>.workers.dev"
TOKEN="<RADAR_TOKEN из шага 2>"

curl -i "$URL/healthz"                     # 204 — Worker жив (без пароля)
curl "$URL/"                               # справка (без пароля, без данных)
curl -X POST "$URL/run?token=$TOKEN"       # проход ВНЕ очереди: весь цикл R2 → радар → R2
curl "$URL/status?token=$TOKEN"            # итог последнего прохода (JSON)
curl "$URL/usage?token=$TOKEN"             # расход и вердикт «A подходит / рекомендую B»
curl "$URL/log?token=$TOKEN"               # лог последнего прохода
curl "$URL/metrics.csv?token=$TOKEN" -o metrics.csv   # срезы расхода — открыть в Excel
```

Логи Worker'а в реальном времени:

```bash
npx wrangler tail
```

Что должно получиться после `POST /run`:

1. в JSON — `"ok": true`, `"exit_code": 0`, `"summary"` со строками радара;
2. в Telegram придут уведомления о находках (если они есть) и ответ бота на `/status`;
3. в R2 обновятся `db/hits.sqlite3` и `logs/last-run.log`;
4. бот-панель на этой же базе — см. следующий раздел.

---

## Шаг 6.5. Бот-панель на облачной базе (по желанию, 1 минута)

Облачный радар шлёт находки в Telegram, но команды бота (`/status`, `/accounts`, `/usage`,
`/sources`, `/errors`) отвечают по базе. База лежит в R2, поэтому панель запускается на её снимке:

```bat
cloud_panel.bat              :: Windows: скачать базу из R2 и запустить панель
cloud_panel.bat --pull-only  :: только скачать cloud_hits.sqlite3
```

```bash
./cloud_panel.sh             # Linux/macOS: то же самое
```

Ключи аккаунта Telegram панели не нужны (она не открывает `.session`), а `TG_BOT_TOKEN` и
`TG_NOTIFY_CHAT` берутся из твоего `.env`. Цифры — на момент скачивания: за свежими запусти
скрипт ещё раз.

Живость в схеме B определяется так же, как в схеме A: **разовый проход пишет пульс** (накопительные
суммы из базы, а не счётчики процесса — контейнер каждый раз новый). Поэтому:

* `/accounts` покажет «работает», пока cron проходит раз в 10 минут;
* если проходы встали (сломалась сессия, Worker не поднимает контейнер), панель поднимет
  «⚠️ молчит N мин» — по умолчанию после 30 минут без пульса (`--alert-silent`);
* «прочитано за час» считается как разница пульсов, то есть суммарно по всем проходам за окно.

---

## Шаг 7. Расписание и деньги

Cron задан в `wrangler.jsonc`: `*/10 * * * *` — проход раз в 10 минут. Смотреть и править:
**Settings → Triggers**.

| Что поменять | Эффект |
|---|---|
| `"*/30 * * * *"` в `wrangler.jsonc` | проход раз в полчаса: задержка находок до 30 минут, контейнер спит больше, дешевле |
| `sleepAfter` в `src/index.js` | через сколько простоя контейнер засыпает. **Не ставь меньше длительности прохода**: фоновая работа внутри контейнера таймер простоя не сбрасывает, только входящие запросы |
| `instance_type` в `wrangler.jsonc` | `lite` = 1/16 vCPU, 256 МиБ, 2 ГБ. Замеры этапа 2: радару нужно ~60 МБ, запас четырёхкратный |
| `RADAR_ARGS` (vars) | аргументы прохода: например `--once --catchup 0 --notify bot --mode B --forward-max-per-day 180` |

Ориентир по деньгам (тарифы Cloudflare, сентябрь 2026): подписка Workers Paid $5/мес включает
25 ГиБ-ч памяти, 375 vCPU-минут и 200 ГБ-ч диска; сверх — $0.0000025 за ГиБ-секунду памяти,
$0.000020 за vCPU-секунду, $0.00000007 за ГБ-секунду диска. Контейнер `lite`, не спящий круглые
сутки, — это примерно **$1.7/мес сверх подписки**; со сном между проходами меньше. Трафик
радара крошечный, egress (1 ТБ включено) не заметен.

---

## Если что-то не так

| Симптом | Причина | Что делать |
|---|---|---|
| `нет файла сессии: monitor_session` | сессия не загружена в R2 | шаг 3; в ответе `/run` уже есть готовая команда |
| `SignatureDoesNotMatch` / HTTP 403 от R2 | неверный `R2_SECRET_ACCESS_KEY` или `R2_ACCOUNT_ID` | пересоздай токен R2, затем `GET /restart?token=…` |
| `HTTP 404` от R2 при сохранении | бакет не создан или имя не совпадает с `R2_BUCKET` | проверь `npx wrangler r2 bucket list` |
| `AUTH_KEY_UNREGISTERED` / «сессия сломана» | Telegram увидел вход с дата-центр IP и отозвал ключ | войди заново локально и обнови сессию в R2 (шаг 3) |
| `AuthKeyDuplicatedError` | два процесса на одну сессию: радар на ПК и в облаке одновременно | оставь один. `max_instances: 1` в конфиге защищает от второго контейнера, но не от твоего ПК |
| `/run` отвечает 502, в логах «прогрев» | первый деплой ещё поднимает образ | подожди 3–5 минут, смотри `npx wrangler tail` |
| `таймаут прохода (540 с)` | много чатов или большой `--catchup` | увеличь `RADAR_TIMEOUT`, уменьши `--catchup`, либо сделай cron реже |
| Build failed: `wrangler: command not found` | не установлен Node/npm в сборке | Build command = `npm install`, Deploy command = `npx wrangler deploy` |
| Build прошёл, а контейнер прежний | деплой был в НЕ-production ветку | шаг 4: production-ветка должна быть той, куда пушишь |
| Worker отвечает, `/healthz` 204, а `/status` 403 | не совпадает `RADAR_TOKEN` | тот же токен в секретах Worker'а и в запросе |
| Секрет поменял, а контейнер работает по-старому | переменные передаются при старте контейнера | `GET /restart?token=…`, затем `POST /run?token=…` |

---

## Безопасность (коротко)

* `.session` = полный доступ к аккаунту Telegram. Живёт только в R2, в git и в образ не попадает
  (`.gitignore` + `.dockerignore` — и то и другое проверено офлайн-тестом).
* Все данные Worker отдаёт только с `RADAR_TOKEN`; без токена отвечают `/healthz` и справка.
  Если секрет `RADAR_TOKEN` не задан, Worker отвечает 500 с подсказкой — молча данные не отдаёт.
* Секреты — только `wrangler secret put` или дашборд, никогда не `vars` в `wrangler.jsonc`.
* Токен R2 создавай с правом «Object Read & Write» **на один бакет**, а не на весь аккаунт.

---

## Локальная проверка без Cloudflare

```bash
python3 deploy/selftest_cloud.py      # 86 проверок: подпись R2, состояние, проход, эндпоинты, конфиг
python3 selftest_monitor.py           # 152 проверки конвейера (включая пульс разового прохода)
python3 selftest_panel.py             # 86 проверок бот-панели (включая живость в схеме B)
python3 selftest_metrics.py           # 65 проверок метрик расхода
python3 selftest.py                   # 7 быстрых проверок расчётов
```

`deploy/selftest_cloud.py` не выходит в интернет: R2 подменяется локальным сервером, который
**пересчитывает подпись** так же, как настоящий R2, запуск радара подставляется функцией.
Отдельно проверяется эталонный вектор подписи AWS — если он сходится, R2 примет наши запросы.

Образ можно собрать и погонять локально (нужен Docker):

```bash
docker build --platform linux/amd64 -t radar .
docker run --rm -p 8080:8080 \
  -e TG_API_ID=... -e TG_API_HASH=... -e TG_BOT_TOKEN=... -e TG_NOTIFY_CHAT=... \
  -e R2_ACCOUNT_ID=... -e R2_BUCKET=radar-state \
  -e R2_ACCESS_KEY_ID=... -e R2_SECRET_ACCESS_KEY=... -e RADAR_TOKEN=test-token \
  radar
curl -X POST "http://127.0.0.1:8080/run" -H "x-radar-token: test-token"
```

---

## Что добавить позже (по желанию)

* **Схема A в контейнере**: `sleepAfter` больше, `RADAR_ARGS` без `--once`, cron не будит,
  а только проверяет живость. Дороже (контейнер не спит) и требует `renewActivityTimeout()` —
  фоновая работа таймер простоя не сбрасывает.
* **Уведомления о провале прохода**: сейчас итог пишется в лог Worker'а, а бот-панель шлёт
  алерты по пульсу; можно добавить алерт «проход не удался N раз подряд».
* **Вторая копия для отладки**: отдельный Worker с `--env staging` и своим бакетом, чтобы не
  трогать боевую сессию.
