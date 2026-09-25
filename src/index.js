/**
 * Worker-обёртка над радаром в Cloudflare Containers (схема B: проход по расписанию).
 *
 * Зачем два слоя. Сам радар — Python (Telethon, SQLite), а Workers исполняют только
 * JavaScript/TypeScript/WASM: Python там урезан до стандартной библиотеки, Telethon не
 * ставится. Поэтому Python живёт в контейнере (любой Docker-образ, linux/amd64), а Worker
 * — это входная точка и диспетчер жизни контейнера: budит его по cron, передаёт секреты,
 * проксирует проверки и не пускает посторонних.
 *
 * Как это работает:
 *   cron каждые 10 минут  ->  scheduled()
 *     -> поднять/разбудить контейнер и дождаться порта 8080
 *     -> POST http://localhost/run   (контейнер: R2 -> monitor.py --once -> R2)
 *     -> залогировать итог; wall-clock лимит cron-обработчика 15 минут, проход успевает
 *   HTTP (с токеном)     ->  /status /usage /metrics.csv /log /run
 *
 * Секреты и переменные задаются в дашборде Worker'а (Settings -> Variables & Secrets)
 * или через `npx wrangler secret put <ИМЯ>`; список — в DEPLOY.md.
 */
import { Container, getContainer } from '@cloudflare/containers';

/** Один и тот же экземпляр контейнера на весь радар: состояние (сессия, база) одно. */
const CONTAINER_ID = 'radar-main';
const CONTAINER_PORT = 8080;
/** Сколько ждём готовности порта: холодный старт образа + импорт Telethon. */
const PORT_READY_TIMEOUT_MS = 60_000;

export class RadarContainer extends Container {
	defaultPort = CONTAINER_PORT;
	/**
	 * Контейнер засыпает после простоя. Ставим больше длительности прохода: фоновая работа
	 * внутри контейнера таймер простоя НЕ сбрасывает, только входящие запросы, поэтому
	 * короткий sleepAfter убил бы радар посреди прохода.
	 */
	sleepAfter = '15m';
	/** Без выхода в интернет радар не достучится ни до Telegram, ни до R2. */
	enableInternet = true;
	pingEndpoint = 'localhost/healthz';
	entrypoint = ['python', '-u', 'deploy/cloud_entry.py'];

	onStart() {
		console.log('[radar] контейнер стартовал');
	}

	onStop({ exitCode, reason }) {
		console.log(`[radar] контейнер остановлен: code=${exitCode} reason=${reason}`);
	}

	onError(error) {
		console.error('[radar] ошибка контейнера:', error?.message ?? error);
		throw error;
	}
}

/** Что передаём в контейнер: ключи Telegram, доступ к R2 и аргументы прохода. */
function containerEnv(env) {
	return {
		TG_API_ID: String(env.TG_API_ID ?? ''),
		TG_API_HASH: String(env.TG_API_HASH ?? ''),
		TG_BOT_TOKEN: String(env.TG_BOT_TOKEN ?? ''),
		TG_NOTIFY_CHAT: String(env.TG_NOTIFY_CHAT ?? ''),
		TG_SESSION: String(env.TG_SESSION ?? 'monitor_session'),
		R2_ACCOUNT_ID: String(env.R2_ACCOUNT_ID ?? ''),
		R2_BUCKET: String(env.R2_BUCKET ?? 'radar-state'),
		R2_ACCESS_KEY_ID: String(env.R2_ACCESS_KEY_ID ?? ''),
		R2_SECRET_ACCESS_KEY: String(env.R2_SECRET_ACCESS_KEY ?? ''),
		RADAR_TOKEN: String(env.RADAR_TOKEN ?? ''),
		RADAR_ARGS: String(env.RADAR_ARGS ?? '--once --catchup 0 --notify bot --mode B'),
		RADAR_TIMEOUT: String(env.RADAR_TIMEOUT ?? '540'),
	};
}

function authHeaders(env) {
	return env.RADAR_TOKEN ? { 'x-radar-token': String(env.RADAR_TOKEN) } : {};
}

/** Поднять (если спит) и дождаться порта. Вызов идемпотентен. */
async function ready(env) {
	const container = getContainer(env.RADAR, CONTAINER_ID);
	await container.startAndWaitForPorts({
		ports: [CONTAINER_PORT],
		startOptions: { envVars: containerEnv(env), enableInternet: true },
		cancellationOptions: { portReadyTimeoutMS: PORT_READY_TIMEOUT_MS },
	});
	return container;
}

/** Один проход радара: ответ контейнера отдаём как есть (JSON с итогом). */
async function runPass(env) {
	const started = Date.now();
	const container = await ready(env);
	const response = await container.containerFetch('http://localhost/run', {
		method: 'POST',
		headers: authHeaders(env),
	});
	const text = await response.text();
	const seconds = ((Date.now() - started) / 1000).toFixed(1);
	console.log(`[radar] проход за ${seconds} с, HTTP ${response.status}`);
	console.log(text.slice(0, 3000));
	return new Response(text, {
		status: response.status,
		headers: { 'content-type': 'application/json; charset=utf-8' },
	});
}

/** Прочие эндпоинты контейнера — проксируем с нужным content-type. */
async function proxy(env, path) {
	const container = await ready(env);
	const response = await container.containerFetch(`http://localhost${path}`, {
		method: 'GET',
		headers: authHeaders(env),
	});
	const types = {
		'/status': 'application/json; charset=utf-8',
		'/metrics.csv': 'text/csv; charset=utf-8',
	};
	const body = await response.text();
	return new Response(body, {
		status: response.status,
		headers: { 'content-type': types[path] ?? 'text/plain; charset=utf-8' },
	});
}

function text(body, status = 200) {
	return new Response(body, { status, headers: { 'content-type': 'text/plain; charset=utf-8' } });
}

const HELP = `Telegram-радар (схема B: проход по расписанию).

Контейнер просыпается по cron, делает один проход по чатам и засыпает.
Состояние (.session и hits.sqlite3) хранится в R2 — диск контейнера эфемерен.

С токеном (?token=<RADAR_TOKEN> или заголовок x-radar-token):
  POST /run         — проход вне очереди
  GET  /status      — итог последнего прохода (JSON)
  GET  /usage       — расход и вердикт «A подходит / рекомендую B»
  GET  /metrics.csv — срезы расхода (открывается в Excel)
  GET  /log         — лог последнего прохода
  GET  /restart     — остановить контейнер (нужно после смены секретов)
Без токена отвечает только /healthz.
`;

const PROTECTED = ['/run', '/status', '/usage', '/metrics.csv', '/log', '/restart'];

export default {
	/** Cron: wall-clock до 15 минут, поэтому дожидаемся прохода целиком и пишем итог в лог. */
	async scheduled(event, env) {
		try {
			await runPass(env);
		} catch (error) {
			// Aлерт уйдёт и в Telegram (бот-панель следит за пульсом), но лог cron тоже важен.
			console.error('[radar] проход по расписанию не удался:', error?.message ?? error);
		}
	},

	async fetch(request, env) {
		const url = new URL(request.url);
		const path = url.pathname.replace(/\/+$/, '') || '/';

		if (path === '/healthz' || path === '/ping') return new Response(null, { status: 204 });
		if (path === '/') return text(HELP);

		if (!PROTECTED.includes(path)) return text(`нет такого пути: ${path}\n\n${HELP}`, 404);

		if (!env.RADAR_TOKEN) {
			return text('Worker не настроен: задай секрет RADAR_TOKEN\n'
				+ '  npx wrangler secret put RADAR_TOKEN\n', 500);
		}
		const token = url.searchParams.get('token') ?? request.headers.get('x-radar-token') ?? '';
		if (token !== String(env.RADAR_TOKEN)) return text('нужен ?token=<RADAR_TOKEN>\n', 403);

		try {
			if (path === '/run') {
				if (request.method !== 'POST') return text('/run принимает только POST\n', 405);
				return await runPass(env);
			}
			if (path === '/restart') {
				// Переменные окружения передаются контейнеру при старте: после смены секретов
				// его надо остановить, иначе он продолжит работать со старыми ключами.
				const container = getContainer(env.RADAR, CONTAINER_ID);
				await container.stop();
				return text('контейнер остановлен — следующий cron или /run поднимет его '
					+ 'с новыми переменными\n');
			}
			return await proxy(env, path);
		} catch (error) {
			const message = error?.message ?? String(error);
			console.error(`[radar] ${path}: ${message}`);
			return text(`не получилось: ${message}\n\n`
				+ 'Первый деплой контейнера прогревается несколько минут — попробуй ещё раз.\n'
				+ 'Логи: npx wrangler tail и дашборд Workers & Pages -> контейнер -> Logs.\n', 502);
		}
	},
};
