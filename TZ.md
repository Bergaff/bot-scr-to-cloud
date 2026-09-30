# ТЗ: Telegram-радар (v2) — перенос всех фич + бот-панель + метрики расхода + хостинг

**Версия ТЗ:** 2.0 от 24.09.2026
**Заказчик:** пользователь из Минска (BY), Windows 10, командная строка `cmd`
**Что приложить к новому чату:** архив `telegram-scraper.zip` (действующий проект: ~7 300 строк,
144 самотеста зелёные, две схемы запуска) — **это исходная точка, а не то, что надо писать с нуля**

---

## 0. Как пользоваться этим ТЗ (инструкция для нового чата)

1. Приложи к новому чату файл `telegram-scraper.zip` — там весь текущий код, документация и тесты.
2. Вставь это ТЗ целиком.
3. Задача для исполнителя формулируется так:

> Возьми проект из архива как есть. **Ничего в существующем поведении не терять** — это проверяется
> прогоном `python selftest_monitor.py` (должно быть `ИТОГ: всё ок`, 144 PASS и больше).
> Работай этапами по разделу 19. Сначала убедись, что перенос 1:1 работает (этап 0), затем
> реализуй разделы 14 (бот-панель) и 15 (метрики расхода), затем 16 (хостинг, схемы A/B).
> По каждой новой возможности: тест в `selftest_monitor.py`, строка в `COMMANDS.md`,
> описание в `START-HERE.md`, и готовая команда для Windows `start.bat ...` в ответе.

4. Пользователь проверяет результат так: распаковка архива в чистую папку → `start.bat --doctor` →
   `python selftest_monitor.py` → `start.bat --once --catchup 20` → бот-панель.

---

## 1. Что это за проект и зачем

**Радар объявлений в Telegram.** Пользователь занимается доставкой/передачей посылок между
Беларусью, Польшей и Литвой. Ему нужно ловить в тематических чатах сообщения вида «еду
Брест→Варшава, возьму передачу» / «нужно передать посылку в Гданьск» — и **мгновенно** пересылать
их своему боту `@parcel_transfer_bot` (своим аккаунтом, настоящим forward), чтобы успеть
отреагировать раньше других.

Ключевое требование: **свежесть важнее полноты** — старое и уже рассмотренное не идёт в уведомления,
шум отсекается, лишние пересылки не тратятся.

Что уже сделано (это надо перенести 1:1, не переделывая):

* чтение 10+ чатов/каналов, включая приватный чат по ссылке-приглашению;
* матчер с категориями, интентами, направлениями и профилями источников;
* дедупликация (по ID сообщения и по тексту, окно и область действия настраиваются);
* отсев авторов со скрытым профилем («hidden by user»);
* окно свежести `--max-age` (по умолчанию 24 ч);
* уведомления: консоль / файл `hits.log` / сообщением от бота;
* пересылка найденного (forward, не копия) с дневным лимитом, очередью отложенных и добором;
* живые сутки по локальной полуночи (Минск), автообнуление лимита;
* статистика `stats.txt` + `stats.csv` (откуда и сколько сообщений идёт);
* экспорт находок в TXT/CSV со ссылками;
* живые форум-темы (`topics`), пульс «я жив», вход по QR без SMS, диагностика `--doctor`,
  понятные ошибки (в т.ч. 401 для бота), догon неотправленных уведомлений;
* случайный порядок обхода чатов (чтобы при лимите не голодали чаты в конце списка);
* **несколько аккаунтов**: у каждого свой `.session`, свои чаты и свой лимит, база общая;
* 144 самотеста без сети и без Telegram.

**Новое в v2 (то, ради чего этот ТЗ):**

* **A. Бот-панель** (раздел 14) — видеть через бота в Telegram статистику и состояние аккаунтов:
  работает ли каждый аккаунт, когда последний раз что-то ловил, что в очереди, что с ошибками.
* **B. Метрики расхода** (раздел 15) — понять, «сколько жрёт» живой слушатель (схема A), чтобы
  принять решение о переходе на проходы по расписанию (схема B).
* **C. Хостинг** (раздел 16) — вынести радар с домашнего ПК: сначала оценить ресурсы, потом
  переезд (Cloudflare Containers lite ≈ $2/мес сверх уже оплаченных $5, либо бесплатные варианты).

---

## 2. Пользователь и окружение (учитывать в каждом решении)

| Параметр | Значение |
|---|---|
| ОС | Windows 10, работает в `cmd` (не PowerShell), путь проекта `C:\Users\User\Desktop\telegram-scraper` |
| Python | 3.10+ (активный `.venv` внутри проекта), на серверах — Linux |
| Ключи | `TG_API_ID`, `TG_API_HASH` — в `.env` (вводятся один раз), `start.bat` их подхватывает |
| Бот | `@parcel_transfer_bot`, диалог активен (Start нажат) |
| Уведомления | бот (`--notify bot`) — основной режим; консоль — для отладки |
| Стиль общения | просит пошаговые инструкции, любит готовые команды для cmd, результаты — файлами (TXT/CSV) |
| Язык | все сообщения скриптов, документация и отчёты — **на русском** |

**Правила работы (из накопленного опыта, соблюдать):**

1. Всегда давать готовую команду Windows `start.bat ...` вместе с новой возможностью.
2. Флаги/поведение документировать **по коду** (argparse), а не по памяти; сверять с кодом.
3. Никогда не утверждать содержимое файлов пользователя без проверки (был конфуз: сказал «в yaml
   100», у него было 150).
4. Не перекрывать настройки конфига дефолтами CLI: `default=None` + отдельная функция разрешения
   приоритетов. Противоречие внутри одного лога — сначала искать CLI-дефолты.
5. Свежая распаковка архива должна работать «из коробки» (проверять на чистой папке).
6. Лимит пересылок по умолчанию **180/сутки**, выше ~200 не советовать.
7. Сутки считаются от **местной полуночи** (не UTC).
8. Одна сессия Telegram = один запущенный процесс (иначе `AuthKeyDuplicatedError` и повторный вход).
9. Не обещать «бесплатный 24/7» без оговорок и не советовать переносить Telethon на Cloudflare
   Workers (V8/JS, 128 МБ — не поедет).

---

## 3. Предметная область: что считается находкой

**Категории сообщений:**

| Код | Смысл | Пример |
|---|---|---|
| `parcel` | посылка/передача/документы/груз/лекарства | «еду Брест–Варшава, возьму передачу» |
| `ride` | попутчики-люди (пассажиры) | «есть 2 места, Минск–Варшава» |
| `mixed` | и посылки, и попутчики (водители с грузом берут передачи) | «везу груз, возьму посылку» |
| `any` | общий сигнал без явной категории | — |

**Интенты:** `offer` (предлагаю — водитель/перевозчик) и `request` (ищу — тот, кому надо передать).

**Направления:** `BY->PL`, `PL->BY`, `BY->LT`, `LT->BY`, `?->PL`, `PL->?`, `?` — определяются по
гео-токенам (страны, города, погранпереходы: Брузги, Кузница, Тересполь, Бобровники и т.д.).

**Шум, который надо гасить:** реклама/промокоды/казино, ТВ-передачи («прямая передача»),
вакансии и «ищу работу», «нужны водители/курьеры», новостные посты про очереди и рейсы
(для профиля `news`).

**Полезно знать про языковой корпус:** сообщения — смесь русского с польскими/литовскими
вкраплениями; матчер работает по нормализованному тексту (регистр, ё, пробелы).

---

## 4. Текущее состояние проекта (что уже есть в архиве)

Файлы (29 в архиве, без служебных):

| Файл | Строк | Роль |
|---|---|---|
| `monitor.py` | 1875 | ядро: конфиг, база, конвейер, живые циклы, CLI, multi-account, доктор |
| `matcher.py` | 311 | правила: категории/интенты/направления, скоринг, профили |
| `core_telegram.py` | 458 | Telethon-обвязка: клиент, paced-вызовы, ссылки, темы, скрытые авторы, формат уведомлений, Bot API |
| `forwarder.py` | 188 | пересылка: forward/copy, лимит, очередь, FloodWait, статусы |
| `login_qr.py` | 254 | вход по QR без SMS, `--session` для второго аккаунта |
| `scraper.py` | 295 | отдельный инструмент: разовый сбор истории/поиск |
| `web_search.py` | 129 | поиск в публичных каналах без аккаунта |
| `selftest.py` | 132 | быстрые проверки расчётов |
| `selftest_monitor.py` | 1414 | 144 проверки конвейера, пересылки, скрытых авторов, аккаунтов |
| `sources.yaml` | 145 | основной конфиг (чаты, аккаунты, лимиты) |
| `sources.json` | 89 | резервный конфиг (если нет pyyaml) |
| `start.bat` / `start.sh` | 84 / 43 | лаунчеры: `.env`, установка зависимостей, проброс флагов |
| `COMMANDS.md` | 252 | полный справочник команд (46 флагов, 10 разделов) |
| `START-HERE.md` | 1131 | пошаговое руководство для пользователя |
| `README.md` | 336 | обзор проекта |
| `HOSTING.md` | 169 | варианты круглосуточного хостинга, цены, ограничения |
| `requirements.txt` | 4 | `telethon`, `pyyaml`, `qrcode`, `pysocks` |
| `examples/`, `tests/` | — | примеры hits/stats + размеченные корпуса для калибровки |

**Зависимости:** `telethon>=1.36,<2`, `pyyaml>=6.0`, `qrcode>=7.4`, `pysocks>=1.7`.
Никаких фреймворков (никаких aiogram/aiohttp/sqlalchemy) — только stdlib + перечисленное.
Новое (опциональное, для метрик): `psutil` — MUST быть **необязательным**, радар работает и без него.

**Что уже работает и проверено живьём:** лимит 180/сутки, местная полночь, очередь и добор,
случайный порядок обхода, форум-темы, приватный чат по инвайту, скрытые авторы (13 тестов),
фикс 401 для бота, догон уведомлений, доктор, два аккаунта (split чатов, отдельные лимиты,
общая база), вход по QR в отдельную сессию.

---

## 5. Архитектура и правила кодирования

**Слои:**

```
sources.yaml / sources.json
        │ load_config()
        ▼
Source[] + defaults ──resolve_accounts()──► AccountConfig[] ──sources_for_account()──► раскладка чатов
        │                                                                                    │
        ▼                                                                                    ▼
   Monitor(client, store, sources, notifier, paced, ..., account=...)  ◄── по одному на аккаунт
        │   process_message() — конвейер (см. §10)
        ├──► HitStore (SQLite: seen/hits/forwarded/stats/…)
        ├──► notifier (console | file | bot)
        └──► Forwarder (forward/copy, лимит, очередь)
```

**Правила:**

* `monitor.py` — единственная точка входа CLI (`build_parser()` + `main()`), `async_main(args)` — тело.
* Все внешние вызовы Telegram — только через `core_telegram.call(...)`/`Paced` (пауза ≥ `--delay`,
  по умолчанию 2 с, с джиттером; FloodWait переживается, а не падает).
* Работа без сети/аккаунта должна быть возможна: `--doctor`, `--show-stats`, `--export`,
  `selftest*` не требуют ключей.
* Код — Python 3.10+ (`X | None` в аннотациях допустимо), UTF-8, без Windows-специфики в Python
  (пути — через `pathlib`, кодировка файлов — явная `encoding="utf-8"`).
* Никаких «магических» параллельных подключений: параллельность только `asyncio.gather` по
  аккаунтам (по одному клиенту на аккаунт), внутри аккаунта — строго последовательно.
* Новые сущности в БД — через `SCHEMA` + идемпотентную миграцию в `HitStore._migrate()`
  (`ALTER TABLE ... ADD COLUMN`, без `--reset-db`).
* Каждое сообщение пользователю — по-русски, с подсказкой «что сделать», без трейсбеков.

---

## 6. Модули и точные интерфейсы (переносить 1:1)

### 6.1 `core_telegram.py`

```python
def load_dotenv(path=".env", override=False) -> int
def display_name(entity, fallback="аккаунт") -> str
class Paced:                        # wait() — пауза ≥ delay + jitter
def call(factory, paced, retries=4, label="")            # FloodWait/сетевые повторы
def make_client(session, api_id, api_hash, delay=2.0, proxy=None)
def invite_hash(target) -> str | None                    # t.me/+hash → hash
def resolve_targets(client, targets, paced, auto_join=False) -> dict[str, entity]
def peer_id(entity) -> int | None                        # ВАЖНО: -100… для каналов
def message_link(chat_username, chat_id, msg_id) -> str
def topic_of(message) -> int | None
def topic_title_of(message) -> str | None
def collect_topics(client, entity, paced, limit=300) -> list[tuple[int, str, int]]
def hidden_author_reason(message) -> str | None          # «hidden by user» → причина
def format_hit(hit, explain=False) -> str                # текст уведомления (с аккаунтом, если есть)
def notify_console(hit, explain=False) -> None
def notify_file(hit, path) -> None
def bot_error_text(exc) -> tuple[str, bool]              # (текст, фатально ли)
def bot_get_me(token) -> tuple[bool, str]                # проверка токена (getMe)
def notify_telegram_bot(hit, token, chat) -> tuple[str, bool]
def build_notifier(mode, notify_file_path="hits.log", explain=False)   # async-функция → bool
```

### 6.2 `matcher.py`

```python
@dataclass
class Match:
    text: str; score: int = 0
    categories: list[str]; intents: list[str]; hit_labels: list[str]; penalties: list[str]
    countries: list[str]; direction: str = "?"; min_score: int = 4
    explain: bool = False; details: bool = False; border_context: bool = False
    @property
    def matched(self) -> bool       # score ≥ min_score И есть категория И (интент ИЛИ маршрут)

RULES: list[tuple[category, intent, weight, regex, label]]     # взвешенные правила
NEGATIVE: list[tuple[weight, regex, label]]                    # штрафы (реклама, вакансии, ТВ)
GEO: dict[str, list[str]]                                      # BY / PL / LT токены
def norm(text) -> str
def country_of(token) -> str | None
def detect_geo(text) -> tuple[list[str], str]                   # (страны, направление)
def analyze(text, min_score=4, explain=False, profile="chat") -> Match
def analyze_many(texts, min_score=4, explain=False, profile="chat") -> list[Match]
```

### 6.3 `monitor.py`

```python
@dataclass
class Source:
    target: str; title: str = ""; profile: str = "chat"; min_score: int = 4
    catchup: int = 0; enabled: bool = True; topics: tuple[int, ...] = ()
    account: str = ""               # main | second | auto | "" (первый аккаунт)

@dataclass
class AccountConfig:
    name: str; session: str; forward: dict; proxy: str | None = None; enabled: bool = True

def load_config(path) -> tuple[dict, list[Source]]              # yaml → json fallback
def resolve_accounts(args, defaults) -> list[AccountConfig]
def sources_for_account(sources, accounts) -> dict[str, list[Source]]
def order_sources(sources, mode="random", rng=None) -> list[Source]
def resolve_forward_settings(args, defaults) -> dict            # CLI → yaml → встроенные
def build_forwarder(client, account, store, paced, args)        # None, если пересылка не настроена
async def connect_client(client, account, args) -> None         # вход с понятными подсказками
async def print_topics_of_source(client, acc, source, entity, paced, args) -> bool
def titles_by_key(sources) -> dict[str, str]
def build_parser() -> argparse.ArgumentParser
def doctor(verbose=True) -> int
async def main_async(args) -> None
def main() -> None

class HitStore:      # SQLite, все методы см. §7.4
class Monitor:       # конвейер, лови на живых событиях
    async def process_message(message, source) -> dict | None
    async def catch_up(sources) -> None            # история при старте (по source.catchup)
    async def flush_deferred() -> dict             # добор очереди первым делом
    def flush_stats() -> None                      # сброс счётчиков прогона в БД
    def heartbeat_text() -> str
    def on_new_day() -> dict                       # сброс лимита + добор
    async def run() -> None                        # живой режим (A)
    def _target_by_entity(chat_id) -> str | None
```

### 6.4 `forwarder.py`

```python
class Forwarder:
    def __init__(self, client, target, store, paced, mode="forward",
                 max_per_day=100, fallback="link", dry_run=False, account="")
    async def prepare() -> bool            # get_entity получателя + счётчик «уже отправлено сегодня»
    async def send(message, hit, mode_override=None) -> str
    # статусы: forwarded | copied | skipped | limit | duplicate | failed[:причина]
    async def flush_queue() -> dict        # добор отложенных (только своего аккаунта)
```

---

## 7. Данные: схема SQLite (перенести как есть + новые таблицы)

### 7.1 Существующие таблицы

```sql
CREATE TABLE IF NOT EXISTS seen (            -- «это сообщение уже разбирали»
    chat_key TEXT NOT NULL, msg_id INTEGER NOT NULL,
    PRIMARY KEY (chat_key, msg_id)
);

CREATE TABLE IF NOT EXISTS hits (            -- найденные объявления
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_key TEXT, chat_title TEXT, chat_id TEXT, username TEXT,
    msg_id INTEGER, date TEXT, sender_id TEXT, sender_name TEXT,
    text TEXT, score INTEGER, category TEXT, intent TEXT,
    direction TEXT, countries TEXT, hits TEXT, link TEXT,
    found_at TEXT, notified INTEGER DEFAULT 0,
    topic_id INTEGER, topic_name TEXT,
    account TEXT                             -- чей аккаунт нашёл (пусто в одноаккаунтном режиме)
);
CREATE INDEX IF NOT EXISTS idx_hits_chat ON hits(chat_key, msg_id);

CREATE TABLE IF NOT EXISTS text_seen (       -- дедуп текстов внутри чата
    chat_key TEXT NOT NULL, text_hash TEXT NOT NULL, first_seen TEXT NOT NULL,
    PRIMARY KEY (chat_key, text_hash)
);
CREATE TABLE IF NOT EXISTS text_seen_global ( -- дедуп текстов между чатами
    text_hash TEXT PRIMARY KEY, first_seen TEXT NOT NULL, chat_key TEXT
);

CREATE TABLE IF NOT EXISTS forwarded (       -- что и как отправлено
    chat_key TEXT NOT NULL, msg_id INTEGER NOT NULL,
    ok INTEGER DEFAULT 0, mode TEXT, error TEXT, at TEXT, account TEXT,
    PRIMARY KEY (chat_key, msg_id)
);   -- mode: forwarded | copied | queued | dead | dry-run | test

CREATE TABLE IF NOT EXISTS stats (           -- счётчики по дням и чатам
    day TEXT NOT NULL, chat_key TEXT NOT NULL,
    scanned INTEGER DEFAULT 0, matched INTEGER DEFAULT 0, saved INTEGER DEFAULT 0,
    forwarded INTEGER DEFAULT 0, forward_skipped INTEGER DEFAULT 0, forward_failed INTEGER DEFAULT 0,
    filtered INTEGER DEFAULT 0, too_old INTEGER DEFAULT 0,
    duplicates INTEGER DEFAULT 0, text_duplicates INTEGER DEFAULT 0, cross_chat INTEGER DEFAULT 0,
    deferred INTEGER DEFAULT 0, hidden INTEGER DEFAULT 0,
    account TEXT,
    PRIMARY KEY (day, chat_key)
);
```

### 7.2 Новые таблицы (v2, для бот-панели и метрик — см. разделы 14–15)

```sql
CREATE TABLE IF NOT EXISTS heartbeats (      -- «пульс» и события: по ним панель считает живость
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,                        -- UTC ISO
    account TEXT,                            -- main | second | '' (не привязано)
    kind TEXT NOT NULL,                      -- start | pulse | event | forward | error | flood | stop | day
    chat_key TEXT,
    detail TEXT
);
CREATE INDEX IF NOT EXISTS idx_heartbeats_ts ON heartbeats(ts);

CREATE TABLE IF NOT EXISTS errors (          -- журнал ошибок для /errors
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL, account TEXT, kind TEXT, text TEXT
);
CREATE INDEX IF NOT EXISTS idx_errors_ts ON errors(ts);

CREATE TABLE IF NOT EXISTS metrics (         -- срез ресурсов (раз в N минут)
    ts TEXT PRIMARY KEY,
    rss_mb REAL, cpu_percent REAL, uptime_s REAL,
    msgs_total INTEGER, msgs_last_hour INTEGER, api_calls INTEGER,
    db_mb REAL, hits_total INTEGER, forwarded_today INTEGER,
    accounts INTEGER, mode TEXT              -- mode: A (listener) | B (scheduled)
);

CREATE TABLE IF NOT EXISTS bot_state (       -- состояние бот-панели (offset getUpdates и т.п.)
    key TEXT PRIMARY KEY, value TEXT
);
```

**Требования к БД:**

* при открытии: `PRAGMA journal_mode=WAL` и `PRAGMA busy_timeout=5000` — чтобы бот-панель и радар
  могли одновременно читать/писать одну базу (два процесса);
* миграции идемпотентные (`ALTER TABLE ... ADD COLUMN` при отсутствии колонки);
* старые базы (до multi-account) при первом открытии помечают историю как `account='main'` —
  дневной счётчик и статистика не должны обнуляться;
* ретеншн: `heartbeats`/`metrics`/`errors` чистить старше 30 дней (при старте и раз в сутки),
  чтобы файл не пух.

### 7.3 Правила записи

| Событие | Что пишем |
|---|---|
| старт процесса | `heartbeats(kind='start', account=<имя>)` для каждого аккаунта |
| каждый пульс (интервал `--heartbeat`) | `heartbeats(kind='pulse', detail='прочитано N, найдено M')` |
| каждая находка | `heartbeats(kind='event', chat_key=...)` |
| каждая отправка | `heartbeats(kind='forward', account=...)` + `forwarded` |
| ошибка (сессия, FloodWait, 401, недоступный чат) | `errors` + `heartbeats(kind='error'|'flood')` |
| выход | `heartbeats(kind='stop')` |

### 7.4 Интерфейс `HitStore` (полный список, не менять имена)

```python
is_seen(chat_key, msg_id) / mark_seen(chat_key, msg_id)
text_is_duplicate(chat_key, text, window_hours, scope="global") -> (bool, chat_key|None)
save_hit(hit: dict) -> bool                    # True, если новая запись (по chat_key+msg_id)
mark_notified(chat_key, msg_id)
was_forwarded(chat_key, msg_id) -> bool
queue_forward(chat_key, msg_id, error="daily_limit", account=None)
deferred_queue(account=None) -> list[tuple[str,int]]     # только свои отложенные
deferred_count(account=None) -> int
get_hit(chat_key, msg_id) -> dict | None
mark_forwarded(chat_key, msg_id, ok, mode="", error="", account=None)
forwarded_today(account=None) -> int                     # с местной полуночи; None — по всем
bump_stats(chat_key, day=None, account=None, **counters)
stats_report(days=7, titles_from_config=None) -> str     # + раздел «ПО АККАУНТАМ»
stats_csv(path) -> int
pending() / pending_count()                              # уведомления с notified=0
export_txt(path, hours=0.0, limit=0) / export_csv(path)  # ссылки на сообщения
stats() -> str                                           # краткая сводка в лог
```

---

## 8. Конфигурация `sources.yaml` (полная схема)

```yaml
defaults:
  profile: chat             # profile по умолчанию: chat | news
  min_score: 5              # порог совпадения (в чатах объявлений на практике 4–5)
  catchup: 50               # сколько последних сообщений читать при старте
  heartbeat: 0              # пульс живого режима, минут (0 — молча; рекомендуем 15)
  skip_hidden: true         # не брать авторов со скрытым профилем («hidden by user»)
  order: random             # random | config — порядок обхода чатов

forward:                    # общие настройки пересылки (аккаунт может переопределить)
  to: "@parcel_transfer_bot"
  mode: forward             # forward — честная пересылка; copy — текст со ссылкой
  max_per_day: 180          # лимит отправок в сутки с местной полуночи (0 — без лимита)
  fallback: link            # если пересылка запрещена: link | skip

accounts:                   # необязательно: несколько аккаунтов
  main:
    session: monitor_session
    forward: {to: "@parcel_transfer_bot", mode: forward, max_per_day: 120}
    # proxy: socks5://127.0.0.1:1080
    # enabled: false
  second:
    session: second_session
    forward: {to: "@parcel_transfer_bot", mode: forward, max_per_day: 60}

sources:
  - target: "@travelersminsk"      # @username | t.me/... | https://t.me/+hash | -100…
    title: "Посылки / Попутчики"   # как показывать в отчёте
    profile: chat                  # chat | news
    min_score: 4                   # переопределяет defaults
    catchup: 50
    topics: [502]                  # только для форум-чатов: читать только эти темы
    account: auto                  # main | second | auto | (пусто → первый аккаунт)
    enabled: true
```

**Приоритеты (жёсткое требование, проверяется тестами):**

* настройки запуска: **флаг CLI → ключ источника → `defaults` → встроенное значение**;
* пересылка: **`accounts.<имя>.forward` → секция `forward` → встроенные (forward/100/link)**;
  CLI-флаги `--forward-*` перекрывают **только первый** аккаунт;
* `catchup`: флаг CLI побеждает конфиг (это фича, не баг — но в логе должно быть видно,
  откуда взято значение: строка `[i] конфиг: <abs-путь> (yaml), источников N, аккаунтов K (…),
  лимит пересылок N/сутки, порядок обхода X`);
* если `sources.yaml` и `sources.json` разошлись по лимиту/порядку — предупреждение `WARN`.

**Текущие источники пользователя (10):** `@travelersminsk`, `@belgranica`, приватный
`https://t.me/+CmQyl50rf-NlODFi` (приоритетный, `catchup: 100`, `min_score: 4`), `@granica_online`,
`@ByPoland_chat`, `@granica_polska`, `@granica_BY_LT_PL`, `@granica_es` (news), `@granica_bypl`
(news), `@rossia_belarus`. Все — с подпиской; проверять вручную не нужно, конфиг только
редактируется текстом.

---

## 9. CLI: все флаги (46; документировать новые так же)

| Флаг | По умолчанию | Что делает |
|---|---|---|
| `--config` | `sources.yaml` | файл со списком чатов |
| `--channels` | — | быстрый запуск `@chat1,@chat2` (**дополняет** конфиг, не заменяет) |
| `--db` | `hits.sqlite3` | файл базы |
| `--session` | `monitor_session` | файл сессии (когда нет секции `accounts`) |
| `--api-id` / `--api-hash` | из `.env` | ключи my.telegram.org |
| `--proxy` | — | `socks5://user:pass@host:port` |
| `--delay` | `2.0` | пауза между вызовами API, сек (ниже 2 не снижать) |
| `--once` | off | разовый проход и выход (**схема B**) |
| `--catchup N` | из конфига | сколько последних сообщений читать при старте |
| `--min-score N` | из конфига | порог совпадения |
| `--profile chat\|news` | из конфига | профиль источника(ов) |
| `--category parcel,mixed,ride` | все | какие категории брать |
| `--only-intent offer,request` | все | фильтр по намерению |
| `--only-direction BY->PL,...` | все | фильтр по направлению |
| `--max-age N` | `24` | не уведомлять о сообщениях старше N часов (0 — без границы) |
| `--dedup-window N` | `24` | окно (часы), в котором одинаковые тексты — дубль (0 — выкл.) |
| `--dedup-scope global\|chat` | `global` | дубли между чатами или внутри чата |
| `--keep-hidden` | off | не отсекать авторов со скрытым профилем |
| `--include-own` | off | не пропускать свои сообщения |
| `--notify console\|file\|bot\|both\|none` | `console` | куда уведомлять |
| `--notify-file` | `hits.log` | файл уведомлений |
| `--explain` | off | показывать, какие правила сработали (примеры, ≤5 ссылок на причину) |
| `--export hits.txt\|csv` | — | выгрузить найденное и выйти |
| `--export-hours N` | `0` | для TXT: только за последние N часов |
| `--stats-file` | `stats.txt` | файл статистики (пишется после каждого запуска) |
| `--stats-days N` | `7` | сколько дней в отчёте |
| `--show-stats` / `--stats-only` | off | показать статистику и выйти (ключей Telegram не требует) |
| `--forward-to` | из конфига | получатель пересылки |
| `--forward-mode forward\|copy` | из конфига | способ отправки |
| `--forward-max-per-day N` | из конфига | лимит отправок в сутки (0 — без лимита) |
| `--forward-fallback link\|skip` | из конфига | если пересылка запрещена |
| `--forward-dry-run` | off | «отправить» без реальной отправки (в базу пишется `dry-run`) |
| `--test-forward` | off | переслать последнее сообщение источника и выйти |
| `--test-notify` | off | проверить бота (getMe) и выйти |
| `--resend-pending [N]` | `50` | догнать неотправленные уведомления |
| `--heartbeat MIN` | из конфига | пульс «я жив» раз в N минут (0 — выкл.) |
| `--order random\|config` | из конфига | случайный старт обхода или как в файле |
| `--list-topics` | off | показать темы форум-чатов с id и выйти |
| `--topics-depth N` | `300` | сколько последних сообщений смотреть при поиске тем |
| `--auto-join` | off | вступать по ссылкам `t.me/+…`, если не подписан |
| `--account NAME` | все | запустить только один аккаунт из `accounts` |
| `--reset-db` | off | удалить базу и выйти |
| `--doctor` | off | диагностика окружения/ключей/конфига/сессий |

**Обязательные новые флаги v2 (см. разделы 14–16):**

| Флаг | Назначение |
|---|---|
| `--bot-panel` | поднять бот-панель (приём команд) вместе с радаром |
| `--panel-only` | запустить **только** панель (радар отдельно / не нужен) |
| `--mode A\|B` | режим работы: A — живой слушатель, B — проход по расписанию (для метрик и /status) |
| `--metrics-interval MIN` | как часто писать срез ресурсов в `metrics` (по умолчанию 15 мин; 0 — выкл.) |
| `--digest morning\|off` | утренний дайджест от бота (по умолчанию off) |
| `--alert-silent MIN` | алерт «аккаунт молчит N минут» (по умолчанию 30; 0 — выкл.) |

---

## 10. Конвейер обработки сообщения (точный порядок — не менять)

`Monitor.process_message(message, source)`:

1. `is_seen(chat_key, msg_id)` → **пропуск** (`duplicates+1`);
2. `scanned+1`;
3. свои сообщения (`message.out`) → пропуск (если не `--include-own`);
4. `--max-age`: старше N часов → пропуск (`too_old+1`) — «старое не будим»;
5. текст короче 4 символов / не строка → пропуск (медиа без подписи);
6. `analyze(text, min_score=source.min_score, profile=source.profile)` → нет `matched` → пропуск;
7. фильтр `--category` → пропуск (`filtered+1`);
8. фильтр `--only-intent` → пропуск (`filtered+1`);
9. фильтр `--only-direction` → пропуск (`filtered+1`);
10. **скрытый автор** (`hidden_author_reason`) при `skip_hidden` → пропуск (`hidden+1`); при `--explain`
    печатается до 5 примеров с причиной;
11. дедуп текста (`text_is_duplicate`, окно/область) → пропуск (`text_duplicates+1`,
    при совпадении с другим чатом — ещё `cross_chat+1`);
12. `matched+1`; сборка `hit` (ссылка, тема, `account`);
13. `save_hit` → `saved+1`, если запись новая;
14. уведомление (`notify`) → при успехе `mark_notified`, при провале `notify_failed+1`;
15. пересылка (`forwarder.send`) → `forwarded+1`, при лимите `forward_skipped+1` и попадание в очередь,
    при ошибке `forward_failed+1`;
16. в v2: запись в `heartbeats(kind='event')` (см. §7.3).

**Живой режим (`run()`):** подписка на новые сообщения по всем чатам аккаунта (через
`events`/`NewMessage`), плюс задачи: `_heartbeat_loop()` (пульс) и `_daily_loop()`
(проверка смены суток каждые 60 с → `on_new_day()` → сброс лимита + `flush_deferred()`).
**Режим B (`--once`):** `resolve_targets` → `flush_deferred()` (сначала добор) → `catch_up()`
по всем источникам → `flush_stats()` → выход.

---

## 11. Матчер: требования

* Правила — список `RULES` (категория, интент, вес, regex, метка) и `NEGATIVE` (штрафы).
  Примеры: «посылк…» +3, «бандероль» +3, «передача/передать» +2, «документы» +2,
  «лекарства» +2, «груз» +2, «везу/еду» +…; штрафы: реклама/промокод/казино −5,
  «прямая передача»/«телепередача» −5, «вакансия/резюме» −3, «нужны водители/курьеры» −6.
* Совпадение = `score >= min_score` **и** есть категория **и** (интент **или** явный маршрут).
  Одиночное слово-объект без контекста не должно будить радар.
* Гео: словари городов и погранпереходов BY/PL/LT; направление выводится из пары стран
  (`BY->PL`, `PL->BY`, `?->PL`, …).
* Профиль `news` гасит новостные посты (очереди, рейсы, правила въезда), но **опасен в чатах
  объявлений** — в них слова «очередь», «без очереди», «рейс» часто встречаются в объявлениях
  водителей (об этом предупреждать в документации).
* Калибровка на размеченных корпусах: `tests/labeled_pos.txt`, `tests/labeled_neg.txt`,
  `tests/news_corpus.jsonl` — тесты по ним обязаны оставаться зелёными.

---

## 12. Пересылка: требования

* Только **настоящий forward** своим аккаунтом (`Forwarder.mode="forward"`), не копия; для чатов
  с защитой контента — `fallback`: `link` (текст + ссылка) или `skip`.
* **Лимит в сутки на аккаунт** (`max_per_day`, по умолчанию 180 суммарно): при исчерпании находка
  **не теряется**, а встаёт в очередь (`mode='queued'`) и уходит в следующий прогон/после полуночи.
* Сутки — по **местному времени** (`day_start_utc()` от локальной полуночи).
* Добор: сначала `flush_deferred()` (старейшие первыми), потом обычная работа; сообщение, которого
  уже нет, помечается `dead` (`message_gone`/`hit_missing`) и больше не пытается отправиться.
* Признак «уже отправлено» — `forwarded` (по `chat_key+msg_id`), повторная отправка не делается.
* Ошибки:
  * `FloodWaitError` → спать `e.seconds * 1.2 + 5` и повторить (`call()`);
  * `PeerFloodError` → остановить пересылку на 24–48 ч, предупредить в лог и в бот;
  * 401/403 у Bot API → фатально: сообщение один раз + подсказка про `TG_BOT_TOKEN`, уведомления
    помечаются неотправленными, потом `--resend-pending`;
  * «получатель недоступен» → подсказка «открой чат с ботом и нажми Start».
* `--forward-dry-run` — проверка конвейера без реальной отправки.

---

## 13. Несколько аккаунтов (уже реализовано — перенести и не ломать)

**Модель:** секция `accounts` (имя → `session`, свой `forward`, необязательный `proxy`, `enabled`).
У источников — `account: main|second|auto|пусто`. Аккаунты работают **в одном процессе**:
по одному Telethon-клиенту на аккаунт, мониторы параллельно (`asyncio.gather`), база общая.

**Требования:**

1. `resolve_accounts(args, defaults)` — если секции нет, создаётся единственный `main`
   (`session` из `--session`), поведение как в v1 **без изменений** (проверяется тестом).
2. `sources_for_account()` — раскладка: явный `account` → в этот аккаунт; `auto` → по кругу;
   пусто → первый аккаунт. Ошибка конфига (аккаунт не из `accounts`) — понятный `sys.exit`
   со списком доступных имён.
3. **Независимо у аккаунтов:** дневной счётчик (`forwarded_today(account)`), лимит, очередь
   отложенных (`deferred_queue(account)`), получатель, прокси.
4. **Общее:** база, дедупликация, `seen`, ссылки, статистика (в отчёте — раздел «ПО АККАУНТАМ»:
   аккаунт · прочитано · найдено · новых · переслано · скрытых · сегодня).
5. В логе и уведомлениях: префикс `[main]`/`[second]` (пульс, старт аккаунта, ошибки),
   в уведомлении строка `· аккаунт second` (только когда аккаунтов больше одного),
   в `hits.account` — имя аккаунта.
6. `--account NAME` — поднять только этот аккаунт (для отладки).
7. `--doctor` — блок «аккаунты»: `PASS/WARN <имя>: сессия X.session` (+ подсказка про
   `start.bat --login-qr --session X` при отсутствии файла) и лимит.
8. `start.bat --login-qr --session second_session` → `login_qr.py --session NAME`
   (сессия из argv, иначе `TG_SESSION`, иначе `monitor_session`).
9. Логика «аккаунт без чатов пропускается» с понятной строкой в логе.
10. **Безопасность:** одна сессия — один запущенный процесс. Панель/диагностика **не имеют права**
    поднимать второй клиент с тем же `.session`, пока радар работает (иначе Telegram отзовёт ключ:
    `AuthKeyDuplicatedError`, потребуется новый вход).

---

## 14. НОВОЕ: бот-панель — статистика и состояние аккаунтов через Telegram

**Цель пользователя дословно:** «чтобы я через бот в тг мог видеть статистику и работают ли мои
аккаунты». То есть бот перестаёт быть только «громкоговорителем» и становится пультом.

### 14.1 Что именно должно быть видно

**Общий статус радара:**
* работает/остановлен, режим (A — слушатель / B — проходы), аптайм;
* сколько часов/аккаунтов в работе, сколько сообщений прочитано всего и за последний час;
* сколько находок сегодня, сколько переслано за сутки (по каждому аккаунту и суммарно);
* очередь отложенных (всего и по аккаунтам), самая старая позиция («висит с 21:40»);
* последнее событие (когда и в каком чате), последнее успешное уведомление;
* ошибки за сутки: сколько, какие (последние 5).

**Состояние каждого аккаунта (главное требование):**

| Показатель | Как получаем | Что значит |
|---|---|---|
| живой/молчит | `heartbeats(kind='pulse')` не старше 3× heartbeat-интервала | «работает» / «молчит N мин» |
| последнее событие | `MAX(ts) WHERE kind='event'` | «последний раз ловил в 21:12 (@чат)» |
| последняя пересылка | `MAX(ts) WHERE kind='forward'` | «отправлял в 21:12» |
| счётчик за сутки | `forwarded_today(account)` | «переслано 43/120» |
| чаты | из конфига | «6 чатов, из них недоступны 0» |
| ошибки сессии | `errors(account)` | «AuthKeyDuplicatedError 21:40» |
| FloodWait | `heartbeats(kind='flood', detail='до …')` | «ограничение до 22:10» |
| простой | `heartbeats(kind='stop')` без нового `start` | «остановлен» |

**Требование:** статус аккаунта **не должен зависеть от новых сообщений** в чатах — иначе тихий чат
ночью выглядит как «аккаунт сломался». Поэтому живость определяется **пульсом**
(`_heartbeat_loop` пишет `heartbeats(kind='pulse', account=…)` каждые `--heartbeat` минут, по
умолчанию 15) + `start`/`stop`. Молчание = нет пульса, а не нет находок.

### 14.2 Команды бота (обязательный минимум)

| Команда | Ответ |
|---|---|
| `/help` | список команд, одна строка на команду |
| `/status` | общий статус: режим, аптайм, аккаунты (по строке), находки сегодня, очередь, ошибок за сутки |
| `/accounts` | подробно по каждому аккаунту: статус, последний пульс, последнее событие/пересылка, счётчик/лимит, очередь, ошибки, FloodWait |
| `/stats [N]` | отчёт статистики за N дней (по умолчанию 7) — тот же текст, что `stats.txt`, обрезанный до лимита Telegram (см. 14.5); при обрезке — пометка «полный отчёт: /report» |
| `/report` | прислать файл `stats.txt` **документом** (sendDocument) |
| `/queue` | очередь отложенных: по аккаунтам, сколько и что самое старое (5 примеров со ссылками) |
| `/last [N]` | последние N находок (по умолчанию 5): время, чат, направление, ссылка |
| `/sources` | по каждому чату: последнее прочитанное сообщение, найдено за сутки, ошибок |
| `/errors [N]` | последние N ошибок (по умолчанию 5) с временем и аккаунтом |
| `/usage` | расход ресурсов и метрики (раздел 15): RSS, CPU, аптайм, сообщений/час, размер БД, вердикт по схеме A/B |
| `/ping` | `pong · 0.4 с` (замер задержки до Bot API) |
| `/mode` | какой режим сейчас (A/B), с какого времени, что советует метрика |
| `/digest on\|off` | утренний дайджест (одно сообщение в 09:00 местного: итоги суток) |
| `/export [N]` | прислать `stats.csv` за N дней документом (и/или `hits.txt` за 24 ч) |

Дополнительно (желательно): `/why <ссылка|id>` — «почему взяли/не взяли это сообщение»
(из `hits`/`explain`), `/top` — топ чатов за сутки по находкам, `/limits` — текущие лимиты и
сколько осталось по каждому аккаунту.

### 14.3 Живые проверки сессий — запрет и правильный путь

**Запрещено:** открывать в панели Telethon-клиенты по тем же `.session`, что использует работающий
радар: Telegram отзовёт ключ (`AuthKeyDuplicatedError`) и потребуется вход заново.

**Правильно:**
* «работают ли аккаунты» определяется по **косвенным признакам** (пульс, события, ошибки, FloodWait)
  — этого достаточно: если аккаунт читает чаты и шлёт пульс, он жив;
* живая проверка (`get_me` по каждой сессии) допустима **только** отдельной командой
  `monitor.py --check-sessions` в момент, когда радар **не запущен** (перед запуском/после остановки);
  при попытке запустить её во время работы — предупреждение и отказ;
* панель никогда не пишет в Telegram **от имени аккаунта** — только от бота (Bot API).

### 14.4 Архитектура панели (два режима, оба обязательны)

**Режим 1 (рекомендуемый): встроенный — `--bot-panel`.**
Отдельная `asyncio`-задача в процессе радара (`class BotPanel` в новом файле `bot_panel.py`):
* long-polling `getUpdates` (`timeout=25`, `allowed_updates=["message"]`) через
  `asyncio.to_thread(urllib.request…)` — **без новых зависимостей** (синхронный HTTP в отдельном
  потоке, чтобы не блокировать Telethon-цикл);
* offset хранится в `bot_state` (чтобы после перезапуска не отвечать на старые команды);
* панель читает **ту же** SQLite (нужен WAL, см. §7.2) и имеет доступ к объектам радара
  (аккаунты, счётчики, очередь) через переданные ссылки;
* выключение — `Ctrl+C`, задача отменяется, пишется `heartbeats(kind='stop')`.

**Режим 2: отдельный процесс — `--panel-only` (или `python bot_panel.py`).**
Панель работает сама, читает базу в режиме чтения, отвечает на команды. Нужна, когда радар
крутится на другом сервере/в контейнере, а телефон должен видеть статус. Требование: те же команды,
источник данных — БД (без живых объектов), поэтому `/status` помечает «данные из базы, обновлено N
минут назад».

**Требования к обеим реализациям:**

1. **Авторизация:** реагировать только на `TG_NOTIFY_CHAT` (числовой id владельца). Всё остальное —
   молча игнорировать, но записать в `errors` (`kind='unauthorized'`). Если `TG_NOTIFY_CHAT` — не
   число (канал по `@имени`), панель отказывается стартовать с понятным сообщением: команды
   принимаются только в личке, нужен числовой id.
2. **Устойчивость:** падение Bot API / отсутствие сети не должно ронять радар (панель ловит
   исключения, пишет в `errors`, повторяет с бэкоффом 5→60 с).
3. **Rate limit:** не чаще 1 сообщения/сек, `/usage` и `/stats` — не чаще 1 раза в 5 с.
4. **Идемпотентность:** повторная доставка апдейта (Telegram может повторить) не должна слать ответ
   дважды — проверка `update_id`.
5. **Первый запуск:** при старте панель пишет владельцу `«панель на связи»` (одно сообщение),
   но только если это не перезапуск чаще чем раз в 5 минут (антиспам).
6. **Формат:** plain text (без Markdown/HTML), чтобы не ловить ошибки парсинга; ссылки — как есть,
   Telegram сам сделает их кликабельными. Эмодзи-заголовки допустимы (в проекте уже используется 📦).
7. Панель не отвечает на команды **во время** догона очереди, если это создаёт FloodWait у Bot API
   (маловероятно, но проверить).

### 14.5 Форматы ответов (шаблоны; соблюдать, они проверяются тестами)

`/status`:
```
📡 Радар · режим A (слушатель) · жив 6 ч 12 мин
Аккаунты: 2 · чаты: 10 · прочитано всего: 41 208 (за час: 318)

Сегодня (с 00:00):
  найдено 37 · переслано 31/180 · в очереди 6
  main: 22/120 · second: 9/60
Последнее событие: 21:12 · @granica_polska · передача в Варшаву
Ошибки за сутки: 1 (очередь: см. /errors)
Обновлено: 21:14
```

`/accounts`:
```
👥 Аккаунты (2)

1) main · работает
   пульс 21:13 (1 мин назад) · сессия monitor_session
   чатов 6 · последнее событие 21:12 (@granica_polska)
   переслано сегодня 22/120 · в очереди 4
   ошибок за сутки 0

2) second · молчит 34 мин ⚠️
   последний пульс 20:40 · сессия second_session
   чатов 4 · последнее событие 19:58 (@granica_BY_LT_PL)
   переслано сегодня 9/60 · в очереди 2
   последняя ошибка 20:41 · FloodWait (до 21:10)
```

`/usage` (см. §15.4).

### 14.6 Алерты (активные сообщения от бота)

| Алерт | Условие | Антиспам |
|---|---|---|
| «аккаунт молчит» | нет пульса больше `--alert-silent` (по умолчанию 30 мин) | не чаще 1 раза в час на аккаунт |
| «сессия сломана» | `AuthKeyDuplicatedError`/`PhoneNumberBannedError`/`SessionRevoked` | однократно |
| «FloodWait» | `PeerFlood` или длинный FloodWait | однократно на событие |
| «очередь растёт» | очередь > 50 и не уменьшается 2 суток | раз в сутки |
| «лимит исчерпан» | дневной лимит выбран, но находки идут | раз в сутки на аккаунт |
| «утренний дайджест» | 09:00 местного (если `/digest on`) | раз в сутки |

Формат алерта — короткий, с командой продолжения: `⚠️ second молчит 34 мин (последний пульс 20:40).
Проверить: /accounts · Подсказка: возможно, сессия занята другим процессом.`

### 14.7 Тесты для панели (обязательны, без сети)

1. Маршрутизация команд: каждая команда из 14.2 даёт непустой ответ и не падает на пустой базе.
2. `/accounts` корректно считает «молчит N мин» (пульс 40 минут назад → статус «молчит»).
3. `/accounts` при отсутствии пульса вовсе → «нет данных о пульсе (радар мог быть запущен без
   --heartbeat)», а НЕ «аккаунт сломан».
4. Обрезка длинных ответов: `/stats` > 4000 символов → обрезка + подсказка `/report`.
5. Whitelist: апдейт с чужим `chat_id` → ответа нет, запись в `errors`.
6. Идемпотентность: один и тот же `update_id` дважды → один ответ.
7. Rate limit: 10 команд подряд → не больше 1 ответа/сек (проверка по фейковому транспорту).
8. `/usage` обращается к метрикам; при отсутствии `psutil` — отвечает частичными данными и
   помечает «psutil не установлен».
9. Панель не поднимает Telethon-клиент ни в одном тесте (проверка: фейковый Telegram-клиент
   считает вызовы, должно быть 0).

---

## 15. НОВОЕ: метрики расхода — «сколько жрёт схема A»

**Цель пользователя:** «я бы проверил сначала A — насколько жрёт, а если много — перешёл бы на B».
Значит нужны **измерения**, а не обещания.

### 15.1 Что измерять (обязательный минимум)

| Метрика | Источник | Зачем |
|---|---|---|
| RSS (память процесса), МБ | `psutil.Process().memory_info().rss`; fallback Linux: `/proc/self/status` `VmRSS`; на Windows без psutil — «н/д» | главный показатель для контейнера |
| Пиковый RSS | максимум за прогон | для выбора класса контейнера (lite = 256 МБ) |
| CPU, % | `psutil` (`cpu_percent`) + средний с момента старта из `time.process_time()/uptime` | понять, хватит ли 1/16 vCPU |
| Аптайм | от старта процесса | расчёт расхода за сутки |
| Сообщений прочитано | счётчики `scanned` (всего и за час) | нагрузка |
| Сообщений/час (пик) | по `stats` за последние часы | вечерний пик 8–12 находок/час — норма |
| API-вызовов Telethon | счётчик в `Paced.call` | оценить FloodWait-риск |
| Размер БД, МБ | `Path(db).stat().st_size` + WAL | диск контейнера (2 ГБ у lite) |
| Находок/пересылок | `hits`, `forwarded` | 180/сутки — потолок |
| Ошибок/FloodWait | `errors` | стабильность |

### 15.2 Как собирается

* Задача `_metrics_loop()` (в `Monitor` или отдельном `MetricsCollector`): раз в
  `--metrics-interval` минут (по умолчанию 15) пишет строку в таблицу `metrics` (§7.2).
* Всё, что можно, — **без внешних зависимостей**: `/proc` на Linux, `resource`/`time` в stdlib.
  `psutil` — опционально (в `requirements.txt` пометить комментарием «опционально»); если его нет,
  поля, которые он даёт, заполняются `None` и панель честно пишет «н/д (нет psutil)».
* Раз в час — строка в `metrics.csv` (человекочитаемый файл рядом с проектом), чтобы пользователь
  мог открыть Excel/`stats.csv`-стилем и посмотреть сутки/неделю. Заголовок:
  `ts,rss_mb,cpu_percent,uptime_h,msgs_total,msgs_last_hour,api_calls,db_mb,forwarded_today,accounts,mode`.
* Ретеншн: `metrics` и `metrics.csv` — 30 дней.

### 15.3 Вердикт «A или B» (автоматический, показывать в `/usage`)

Правила (настраиваемые константами в коде):

| Условие (за последние сутки работы) | Вердикт |
|---|---|
| RSS ≤ 150 МБ и средний CPU ≤ 5% и нет FloodWait/ошибок сессии | «A подходит: ресурсов мало» |
| 150 < RSS ≤ 250 МБ или CPU ≤ 15% | «A возможна, но следи: близко к лимитам lite-контейнера» |
| RSS > 250 МБ или CPU > 15% или частые FloodWait | «рекомендую B (проходы по расписанию)» |

Формулировки — по-русски, с цифрами и датой замера, например:
`Сутки работы: RSS 62 МБ (пик 78), CPU 1.4 %, БД 11 МБ, сообщений 3 480 (пик 12/мин).
Вердикт: A подходит — контейнер lite (256 МБ) с запасом.`

### 15.4 Шаблон `/usage`

```
🧮 Расход и нагрузка · режим A · аптайм 26 ч 40 мин

Память: 62 МБ (пик 78) · процессор: 1.4 % (среднее за сутки)
База: 11.2 МБ (WAL 0.4) · диск под проект: 14 МБ
Сообщений: 3 480 всего · за час 214 · пик 12/мин (20:10)
API-вызовов: 1 902 · FloodWait: 0 · Ошибки: 0
Находок: 61 · переслано сегодня 44/180 (main 28/120, second 16/60) · в очереди 3

Вердикт: A подходит (RSS 62 МБ ≤ 150, CPU 1.4 % ≤ 5)
Сутки: 2026-09-24 → RSS 62, CPU 1.4 %, 3 480 сообщений
```

### 15.5 Тесты метрик

1. Формирование строки `metrics` из фейковых значений (детерминированно, без psutil).
2. Расчёт среднего CPU из `process_time()/uptime` (подстановка значений → ожидаемое число).
3. Вердикт A/B на 4 наборах входных данных (граничные значения 150/250 МБ, 5/15 %).
4. `metrics.csv` — заголовок + строка, разделитель `,`, кодировка utf-8.
5. Отсутствие `psutil` → метрики не падают, поля «н/д», вердикт считается по доступным данным.
6. Ретеншн: старые строки (31 день) удаляются, свежие — нет.

---

## 16. Хостинг: схема A и схема B (как решать и что делать)

### 16.1 Разница

| | **A. Слушатель** | **B. Проходы по расписанию** |
|---|---|---|
| Как работает | процесс висит, `events` ловит новое мгновенно | `--once` каждые 5–10 мин, читает новое `seen`-таблицей |
| Задержка | секунды | до 5–10 минут |
| Ресурсы | постоянно занята память/процесс | почти ноль между запусками |
| Где живёт | Windows-ПК, VPS, контейнер, телефон | Планировщик Windows, cron, GitHub Actions, CF Containers по таймеру |
| Что уже есть в коде | `monitor.py` без `--once` | `--once`, `seen`, `catchup: 0` |

**Решение принимается по метрикам раздела 15:** сначала сутки-двое в режиме A, смотреть `/usage`;
если вердикт «A подходит» — оставить A и перенести в контейнер lite; если «рекомендую B» —
переключиться на `--once` по расписанию (задержка 10 минут для посылок обычно допустима: успеть
написать первым всё равно реально, если читать каждые 10 минут).

### 16.2 Варианты площадок (подробности — в `HOSTING.md` проекта)

| Вариант | Цена | Годится для |
|---|---|---|
| Cloudflare Containers **lite** (1/16 vCPU, 256 МБ, 2 ГБ диск) | ≈ **$2/мес** сверх уже оплаченных $5 | A (если нет проблем со сном контейнера) и B |
| Cloudflare standard-1 (1/2 vCPU, 4 ГБ) | $35–58/мес | не нужно (переплата) |
| Oracle Cloud Always Free | 0 | A и B (полноценная VM) |
| Старый Android + Termux | 0 | A (домашний IP, ничего не переписывать) |
| GitHub Actions по расписанию | 0 | только B |
| Дешёвый VPS $3–7 | $3–7 | A и B (самый простой перенос) |

**Жёсткие требования к любому переезду:**

1. Перед стартом на новом месте — **остановить радар дома** (одна сессия = один процесс).
2. Перенести: `sources.yaml`, `sources.json`, `hits.sqlite3`, `*.session`, `.env`, `stats.txt`
   (как есть — база переносится целиком, история и дедуп сохраняются).
3. Первые запуски на новом месте: `--doctor` → `--test-notify` → `--once --catchup 0` (проверить,
   что видит чаты) → и только потом A или B в боевом режиме.
4. Telegram видит новый IP (для дата-центра): аккаунт может потребовать вход заново; первые сутки
   не выкручивать лимиты, не включать сразу оба аккаунта на максимум.
5. В контейнерах база/сессия должны лежать на постоянном хранилище (у CF — R2/D1 или том); при
   эфемерной ФС `hits.sqlite3` обнулится — радар начнёт слать всё заново.

### 16.3 Готовые способы запуска B

* **Windows (Планировщик задач):** задача каждые 10 минут, программа `start.bat`,
  аргументы `--once --catchup 0 --notify bot --no-pause` (флаг `--no-pause` уже поддерживается,
  задачи планировщика не должны ждать нажатия клавиши).
* **Linux/cron:** `*/10 * * * * cd /opt/radar && ./start.sh --once --catchup 0 --notify bot`.
* **GitHub Actions:** workflow `schedule: cron '*/10 * * * *'`, шаги: checkout → setup-python → 
  `pip install -r requirements.txt` → восстановить `hits.sqlite3`/`.session` из кэша/артефакта или
  R2 → `python monitor.py --once --catchup 0 --notify bot` → сохранить базу обратно.
  Секреты — GitHub Secrets (`TG_API_ID`, `TG_API_HASH`, `TG_BOT_TOKEN`, `TG_NOTIFY_CHAT`);
  `TG_SESSION_STRING` (строка сессии Telethon) предпочтительнее файла `.session`.
* **Cloudflare Containers:** контейнер + Worker-триггер (Cron Triggers) → для B; для A — держать
  инстанс и следить за «sleep after» в настройках класса.

---

## 17. Тесты и критерии приёмки

### 17.1 Тесты (обязательная часть сдачи)

* `python selftest_monitor.py` → **не меньше 144 PASS** и `ИТОГ: всё ок`; все старые проверки
  обязаны остаться (это и есть «перенос всех фишек»);
* `python selftest.py` → `ИТОГ: всё ок`;
* новые группы (обязательны):
  1. **Панель** — 9 тестов из §14.7;
  2. **Метрики** — 6 тестов из §15.5;
  3. **Схема B** — `--once` с пустым списком новых сообщений завершается успешно (exit 0), не
     дублирует отправленное, добор очереди выполняется первым;
  4. **Миграция БД** — старая база (без `account`, без `heartbeats/metrics/errors/bot_state`)
     открывается и получает все таблицы/колонки, история помечается `main`;
  5. **Панель не трогает сессии** — тест из §14.7 п.9;
  6. **Ретеншн** — старые `heartbeats/metrics/errors` чистятся, свежие остаются.
* Тесты должны работать **без сети, без Telegram и без psutil** (фейковый клиент, фейковый
  Bot API-транспорт, подстановка значений).

### 17.2 Критерии приёмки (принимается, если всё выполнено)

- [ ] архив распаковывается в чистую папку, `start.bat --doctor` работает без ключей и без сессии;
- [ ] `python selftest_monitor.py` — ≥144 PASS, `ИТОГ: всё ок`;
- [ ] старые команды работают как раньше (проверить вручную: `--once --catchup 20`,
      `--show-stats`, `--export hits.txt`, `--test-notify`, `--doctor`, `--list-topics`);
- [ ] два аккаунта: `accounts` + `account:` + `--account`; в отчёте раздел «ПО АККАУНТАМ»;
      `--doctor` показывает обе сессии; лимиты/очереди не смешиваются;
- [ ] бот-панель: `/status`, `/accounts`, `/stats`, `/queue`, `/last`, `/errors`, `/usage`, `/ping`,
      `/help` отвечают; `/accounts` показывает, живы ли аккаунты, и честно пишет «молчит N мин»;
- [ ] алерты: остановка пульса (проверяется отключением аккаунта) → приходит предупреждение;
- [ ] `/usage` показывает RSS/CPU/аптайм/сообщения/базу и **вердикт «A подходит / рекомендую B»**;
- [ ] `metrics.csv` заполняется; `metrics`/`heartbeats`/`errors` чистятся по ретеншну;
- [ ] `COMMANDS.md` содержит все новые флаги (`--bot-panel`, `--panel-only`, `--mode`,
      `--metrics-interval`, `--digest`, `--alert-silent`, `--check-sessions`) и раздел про панель;
- [ ] `START-HERE.md` — разделы «Бот-панель: смотреть статистику из Telegram», «Расход: A или B»;
      `README.md` — краткие абзацы; `HOSTING.md` — актуализирован;
- [ ] `TZ.md` (этот файл) остаётся в проекте;
- [ ] в ответе пользователю — **готовые команды для cmd** на каждую новую возможность
      (`start.bat --bot-panel ...`, `start.bat --once ...`, задания планировщика).

---

## 18. Безопасность и анти-бан (не нарушать)

1. **Одна сессия — один процесс.** Никаких параллельных подключений тем же `.session`
   (в т.ч. из панели, диагностики, второго окна). Нарушение → `AuthKeyDuplicatedError` и новый вход.
2. Пауза между вызовами Telegram ≥ `--delay` (по умолчанию 2 с) с джиттером; ниже 2 с не опускать.
3. `FloodWaitError` — спать `seconds * 1.2 + 5` и повторять; `PeerFloodError` — остановка пересылки
   на 24–48 ч, предупреждение в бот, без автоповторов.
4. Не ротировать сессии/аккаунты для обхода ограничений; два аккаунта — это «разные чаты», а не
   «двойной лимит на те же действия». Личный совет в документации: суммарный лимит держать ≤180–200/сутки.
5. Публичные каналы читать безопасно; массовые `GetParticipants` не использовать (высокий риск бана).
6. Вступать в чаты (`--auto-join`) — не больше 1–2 в час, не пачкой.
7. Новый аккаунт/новый IP: первые сутки спокойный режим (прогрев), без 200 пересылок.
8. `--max-age` и `catchup` не задирать до «прочитать всю историю» — это лишняя нагрузка и риск.

---

## 19. План работ (порядок для нового чата)

**Этап 0. Перенос и проверка базы (обязательный первый шаг).**
Распаковать `telegram-scraper.zip`, прогнать `selftest_monitor.py` (ожидание: `ИТОГ: всё ок`,
144 PASS). Ничего не менять до зелёного прогона. Это фиксирует, что все фишки перенесены.

**Этап 1. Панель (раздел 14).**
`bot_panel.py` + `--bot-panel` / `--panel-only`; таблицы `heartbeats`, `errors`, `bot_state`;
запись пульса `_heartbeat_loop` в `heartbeats`; команды из 14.2 (в первую очередь `/status`,
`/accounts`, `/stats`, `/usage`, `/help`); тесты §14.7.
Критерий готовности: пользователь пишет боту `/accounts` и видит статусы обоих аккаунтов.

**Этап 2. Метрики (раздел 15).**
Таблица `metrics`, `_metrics_loop`, `metrics.csv`, вердикт A/B, `/usage` полный, тесты §15.5.
Критерий готовности: после суток работы `/usage` даёт однозначный вердикт.

**Этап 3. Схема B (раздел 16.3).**
Проверить, что `--once` идеально подходит для планировщика: корректный выход с кодом 0, `--no-pause`,
отсутствие интерактивных запросов, минимальный лог. Дать готовые задания: Планировщик Windows, cron,
GitHub Actions.

**Этап 4. Документация и упаковка.**
`COMMANDS.md` (новые флаги), `START-HERE.md` (разделы про панель и про расход), `README.md`,
`HOSTING.md`, обновить `TZ.md` при изменениях, пересобрать zip, проверить на чистой распаковке.

**Этап 5 (по запросу пользователя). Переезд.**
Сначала по метрикам решается A или B; потом площадка (CF Containers lite / Oracle / Termux / VPS).
Требования к переезду — §16.2. Всё, что нужно для CF: Dockerfile, Worker-обёртка, синхронизация
базы и `.session` в R2, инструкция «останови дома → запусти на сервере».

---

## 20. Открытые вопросы и известные риски

| Вопрос | Состояние |
|---|---|
| `catchup: auto` (самообучающийся хвост: первый запуск глубже, дальше мелко) | предложено, не реализовано; решить, нужно ли |
| Sleep контейнера в Cloudflare Containers | нужно проверить на живом аккаунте: усыпляется ли lite-инстанс, рвётся ли соединение (влияет на выбор A/B) |
| FloodWait при двух аккаунтах с одного IP | наблюдать через `/errors`; при частых — развести часы работы или разные прокси |
| Второй аккаунт: нужен ли ему свой получатель | сейчас можно указать отдельный бот/канал; решить по факту |
| Пороги вердикта A/B (150/250 МБ, 5/15 %) | стартовые значения, уточнить после суток реальных замеров |

---

## 21. Приложения

### 21.1 Пример `sources.yaml` целиком (с двумя аккаунтами)

```yaml
defaults:
  profile: chat
  min_score: 5
  catchup: 50
  heartbeat: 15
  skip_hidden: true
  order: random

forward:
  to: "@parcel_transfer_bot"
  mode: forward
  max_per_day: 180
  fallback: link

accounts:
  main:
    session: monitor_session
    forward: {to: "@parcel_transfer_bot", mode: forward, max_per_day: 120}
  second:
    session: second_session
    forward: {to: "@parcel_transfer_bot", mode: forward, max_per_day: 60}

sources:
  - target: "@travelersminsk"
    title: "Посылки 📦 Доставки 🧳 Попутчики 🙋 Водители 🚘"
    min_score: 4
    account: main
  - target: "https://t.me/+CmQyl50rf-NlODFi"
    title: "Приватный чат (приоритетный)"
    min_score: 4
    catchup: 100
    account: main
  - target: "@granica_polska"
    title: "Граница Польша"
    account: second
  - target: "@granica_BY_LT_PL"
    title: "Граница BY-LT-PL"
    account: auto
  - target: "@granica_es"
    title: "Граница BY-PL-LT (очереди)"
    profile: news
    catchup: 30
    account: second
```

### 21.2 Готовые команды для пользователя (Windows cmd)

```bat
REM ── первый запуск на новом месте ───────────────────────────────────────────
start.bat --doctor                                  проверить всё (ключи, сессии, конфиг)
start.bat --login-qr                                вход по QR (первый аккаунт)
start.bat --login-qr --session second_session        вход по QR (второй аккаунт)
start.bat --test-notify --notify both               проверить бота (сообщение в Telegram)
start.bat --test-forward                            проверить пересылку боту
start.bat --once --catchup 20 --category parcel,mixed --max-age 24   пробный проход

REM ── боевой режим A (слушатель) + панель ────────────────────────────────────
start.bat --notify bot --heartbeat 15 --bot-panel --metrics-interval 15 --order random

REM ── режим B (проходы по расписанию) ─────────────────────────────────────────
start.bat --once --catchup 0 --notify bot --heartbeat 0 --no-pause
REM   в Планировщике задач Windows: каждые 10 минут, аргументы:
REM   --once --catchup 0 --notify bot --no-pause

REM ── посмотреть, что происходит ─────────────────────────────────────────────
start.bat --show-stats --stats-days 1                статистика без Telegram
start.bat --panel-only                               только панель (радар отдельно)
start.bat --export hits.txt --export-hours 24         выгрузка находок за сутки
```

### 21.3 Как бот должен отвечать на «работают ли мои аккаунты» — правило одной строкой

Если пользователь спрашивает `/accounts`, ответ обязан содержать по каждому аккаунту три вещи:
**статус** (работает / молчит N мин / остановлен / ошибка), **последнюю активность** (пульс,
последнее событие, последняя пересылка) и **сегодняшние числа** (переслано/лимит, очередь, ошибки).
Не показывать «всё ок», если пульса не было больше интервала heartbeat — это ложь, которая дороже
молчания.

### 21.4 Словарь терминов проекта

| Термин | Значение |
|---|---|
| находка (hit) | сообщение, прошедшее матчер и все фильтры, попавшее в базу |
| пересылка (forward) | отправка находки получателю настоящим forward; альтернатива copy — текст со ссылкой |
| очередь / добор | находки, не ушедшие из-за лимита; уходят в следующий прогон или после местной полуночи |
| пульс | сообщение «я жив» раз в N минут (лог + `heartbeats`) |
| скрытый автор | сообщение с `hidden by user` (нельзя написать) — по умолчанию не берём |
| схема A / B | живой слушатель / проходы по расписанию |
| панель | бот-интерфейс для статистики и статуса аккаунтов (раздел 14) |

---

**Конец ТЗ.** Если исполнитель сомневается между «сделать как было» и «улучшить» — выбирается
«как было» + тест. Всё новое добавляется поверх, без ломки существующих флагов, ключей конфига и
имён таблиц: пользователь работает с этим проектом каждый день и не должен переучиваться.
