#!/usr/bin/env python3
"""
Матчер сообщений: посылки / передачи / попутчики.
Перевод и доработка JS-регexpов из запроса.

Что исправлено по сравнению с исходными KEYWORD_HINTS / PASSENGER_HINTS:
  * 'груз' больше не ловит 'грустно/грустный'   -> груз(?!ст)
  * 'ищу' больше не ловит 'поищу/отыщу/взыщу'   -> (?<![а-я])ищу(?![а-я]) + низкий вес
  * 'переда[мтн]' не ловил 'передача/передай/передают' -> переда[а-я]{0,4}
  * добавлены предложные смыслы ('есть место', 'кто едет', 'нужна машина')
  * добавлены категории (посылка/попутчик), намерение (ищу/предлагаю),
    географию (страна и направление) и вес совпадения, чтобы 'ищу' в одиночку
    не будило уведомление
  * добавлены антипаттерны: реклама, «заявка принята», пересылки новостей

Использование:
  python3 matcher.py --text "везу в Варшаву 20.09, возьму посылку"
  python3 matcher.py --file /tmp/real_messages.json          # список сообщений из web_search.py
  python3 matcher.py --file corpus.txt --min-score 3 --explain
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from dataclasses import dataclass, field

# ------------------------------------------------------------------ правила

# (категория, намерение, вес, паттерн, метка)
RULES: list[tuple[str, str, int, str, str]] = [
    # --- посылки / передачи: объект
    ("parcel", "any", 3, r"посылк[а-я]{0,3}|посыл[ое]чк[а-я]{0,3}|посылок", "посылка"),
    ("parcel", "any", 3, r"бандерол[а-я]{0,3}", "бандероль"),
    ("parcel", "any", 2, r"передач[а-я]{0,3}|переда[а-я]{0,4}", "передача/передать"),
    ("parcel", "any", 2, r"коробк[а-я]{0,3}|короб\b", "коробка"),
    ("parcel", "any", 2, r"документ[а-я]{0,4}", "документы"),
    ("parcel", "any", 2, r"лекарств[а-я]{0,3}|лекарст[а-я]{0,3}|медикамент[а-я]{0,3}", "лекарства"),
    ("any", "any", 2, r"вещ(?:и|ей|ам|ами|ь)\b", "вещи"),
    ("parcel", "any", 2, r"груз(?:ы|а|ов|ам|ами|оперевоз[а-я]{0,3})?\b|груз(?!ст)[а-я]{1,3}\b", "груз"),
    ("parcel", "any", 2, r"курьер[а-я]{0,3}|доставк[а-я]{0,3}|доставить|доставлю|доставим", "курьер/доставка"),
    # слова из живых объявлений: пакет, конверт, запчасть, пачкомат, «оказия» (= попутная передача)
    ("parcel", "any", 2, r"пакет(?:а|у|ы|ов|ом|е)?\b|конверт[а-я]{0,3}|запчаст[а-я]{0,3}|пачкомат[а-я]{0,3}|поручени[а-я]{0,3}|доки\b", "пакет/конверт/запчасть"),
    ("parcel", "any", 3, r"оказ(?:ия|ию|ией|ии)\b", "оказия"),
    # слабые предметы: категорию не задают (чтобы «продам телефон» не будил радар), лишь добавляют вес
    ("any", "any", 1, r"телефон[а-я]{0,3}|айфон[а-я]{0,3}|iphone|зар[яе]дк[а-яё]{0,3}|ключ(?:и|ей|ам)\b|подар(?:ок|ки|ка|ков)\b|украшени[а-я]{0,3}|мелочь|мелочи|вещиц[а-я]{0,3}", "мелкие вещи"),
    ("parcel", "any", 2, r"перевез[а-я]{0,4}|перевозк[а-я]{0,3}", "перевозка"),

    # --- «предлагаю»: слова-намерения нейтральны по категории, предмет задаёт её ниже
    ("any", "offer", 3, r"во[зс]ь?м[уёе]м?|возьмут|берем\b|беру|заберу|заберем\b|подберу|докину|докинем\b|захвачу", "возьму/беру"),
    ("any", "offer", 3, r"(?<![а-я])(?:при|от|за|пере|до)?вез[уёе]м?\b|(?:при|от|за|пере|до)?вез[её]т\b", "везу"),
    ("any", "offer", 3, r"(?:могу|смогу|можем)\s+(?:взять|забрать|передать|перевезти|отвезти|докинуть|привезти|довезти|завезти|захватить|выкупить)", "могу взять"),
    ("parcel", "offer", 3, r"(?:при)?вез[уе]м?\s+(?:\S+\s+){0,3}?(?:что|все|всё)\s+(?:нужно|надо|угодно|попросите|хотите|нужное)|(?:заберу|заберем|докину|передам)\s+(?:вам\s+)?(?:что|все|всё|любую?|любые)\b", "привезу что нужно"),
    ("parcel", "offer", 3, r"(?:кто|желающи[ех])\s+(?:хочет\s+|хотел\s+бы\s+)?(?:что[\s\-]*то\s+)?(?:передать|отправить|переслать)", "кто хочет передать"),
    ("any", "any", 2, r"мест[ао]?\s+для\s+багаж|багаж", "место для багажа"),

    # --- посылки: «ищу» (нужно передать)
    ("parcel", "request", 3, r"(?:нужно|надо|необходимо|хочу|хотела?|прошу|помогите)\s+(?:бы\s+)?(?:срочно\s+)?(?:передать|переслать|отправить|перевезти|довезти|забрать|отвезти|привезти|завезти|доставить)", "нужно передать"),
    ("parcel", "request", 3, r"нужн[аоы]?\s+(?:передач[а-я]*|оказ[а-я]+|доставк[а-я]+|курьер[а-я]*)", "нужна передача/оказия"),
    ("parcel", "request", 3, r"(?:отправ[а-я]+|передать|переслать|передам)\s+(?:\S+\s+){0,3}с\s+(?:попутчик[а-я]*|оказией|водител[а-я]*|кем)", "отправить с попутчиком"),
    ("any", "request", 2, r"кто(?:[\s\-]*(?:нибудь|либо|то))?\s+(?:(?:\S+\s+){0,2}?(?:вез[её]т|повез[её]т|едет|поедет|поедут|летит|полетит|летят|завез[её]т|забер[её]т|передаст|отвез[её]т|привез[её]т|докинет|поможет|выручит|будет\s+(?:ехать|лететь))|может)\b", "кто везёт/едет"),
    ("any", "request", 2, r"(?:есть\s+(?:ли\s+)?|едет\s+ли\s+|летит\s+ли\s+|может\s+быть\s+|может\s+)кто(?:[\s\-]*(?:нибудь|либо|то))?\s+(?:(?:\S+\s+){0,2}?(?:едет|поедет|летит|полетит|вез[её]т|завез[её]т|привез[её]т)|на\s|в\s|из\s)|(?:может\s+(?:быть\s+)?)кто(?:[\s\-]*(?:нибудь|либо|то))?\s+(?:\S+\s+){0,2}?(?:завезти|довезти|передать|отвезти|привезти|забрать|взять|ехать|лететь|поехать|полететь)", "есть кто едет"),
    ("any", "request", 3, r"ищу\s+(?:того|человека|машин[уы]|попутчик[а-я]*|курьер[а-я]*|тех|кто|кого|с\s+кем|оказ[а-я]+)", "ищу кто"),
    ("parcel", "request", 2, r"требуется\s+(?:перевезти|передать|доставка)", "требуется"),

    # --- попутчики / пассажиры
    ("ride", "any", 3, r"пассажир(?:а|у|ы|ов|ам|ами|ом|ей)?\b|попут[а-я]{0,5}", "пассажир/попутчик"),
    ("ride", "offer", 3, r"подве[зс][а-я]{0,4}|довезу|довез[её]т|дове[зс]ти\s+(?:человек|людей|пассажир|родных|меня|нас)|подброшу|подкину", "подвезу"),
    # «забрать X и довести до Y», «за оплату» — просьба передать груз, а не подвезти человека
    ("parcel", "request", 3, r"забрать[^.]{0,50}(?:и\s+)?(?:довести|довезти|доставить|привезти|завезти|передать)|"
                            r"довести\s+до\b|за\s+оплату|за\s+вознагражд|оплачу|отблагодарю", "просьба перевезти груз"),
    ("ride", "offer", 3, r"есть\s+мест[ао]?\b|свободн[а-я]{0,3}\s+мест|мест[аоы]?\s+(?:в|на)\s+машин|пара\s+мест|есть\s+\d+\s+мест", "есть места в машине"),
    ("ride", "request", 3, r"нужн[аоы]?\s+(?:машин[ауы]|бус|микроавтобус|авто|водител[а-я]{0,3})", "нужна машина"),
    ("ride", "request", 3, r"ищу\s+(?:\d+\s+)?(?:мест[ао]|попут[а-я]{0,5}|машин[уы]|водител[а-я]{0,3})|места\s+на\s+\d|мест[ао]?\s+на\s+\d", "ищу место/попутку"),
    ("ride", "any", 2, r"чемодан[а-я]{0,3}|сумк[а-я]{0,3}\b|рюкзак[а-я]{0,3}", "багаж пассажира"),
    ("ride", "offer", 2, r"возьму\s+(?:попутчик|пассажир|людей)|подвезу|довезу\s+(?:человек|людей)", "возьму попутчиков"),

    # --- детали объявления (дата/вес/оплата) — признак реального поста, а не новости
    ("any", "any", 2, r"\b\d{1,2}[.:]\d{2}\b|\b\d{1,2}\s*(?:сент|окт|нояб|дек|янв|фев|мар|апр|ма[йя]|июн|июл|авг)|оплат[а-я]{0,3}|\bцена\b|тариф|\bкг\b|мест[ао]?\b", "детали объявления"),

    # --- слабые сигналы (сами по себе не должны ничего будить)
    ("any", "request", 1, r"(?<![а-я])ищу(?![а-я])", "ищу (слабо)"),
    ("any", "any", 1, r"срочно|сегодня|завтра|до\s+\d{1,2}[.\-/]\d{1,2}", "срочность"),
    ("any", "any", 1, r"пиш[иу]те?\s+в\s+(?:лс|л\.с|личк[уи]|личные|директ)|в\s+л/?с\b", "призыв писать"),
]

# антипаттерны: реклама, служебное, нерелевантный контекст
NEGATIVE: list[tuple[int, str, str]] = [
    (-5, r"подпис(?:ывай|ывайт|ка|ки|ался|ал[ась]|сь)|реклам[а-я]*|прайс|скидк[а-я]{0,3}|акци[яию]|промокод|betting|casino|казино", "реклама"),
    (-5, r"передача\s+(?:на\s+)?(?:канал|эфир|тв)|прямая\s+передача|телепередач", "ТВ-передача"),
    (-4, r"заявка\s+(?:принята|оформлена)|успешно\s+(?:зарегистрирован|оформлен)|ваш\s+заказ\s+принят", "служебное"),
    (-3, r"(?<![а-я])ищу\s+(?:работу|сотрудник|специалист|программист|дизайнер|менеджер)|вакансия|резюме", "вакансии"),
    (-6, r"нужны\s+(?:водител|перевозчик|курьер|люди|сотрудник|работник)|ищем\s+(?:водител|перевозчик|курьер|сотрудник)|требуются\s+(?:водител|перевозчик|курьер)|по\s+оплате\s+не\s+обижу|график\s+гибк|совмещать\s+с\s+основн|на\s+личном\s+автомобил|трудоустрой|зарплат|оплата\s+(?:за\s+)?(?:рейс|смену|час)|подработк|вахт[ае]", "вакансия/подработка"),
    (-4, r"(?:^|[^а-я])(?:вводит|введ[её]т|ввел[аи]?|запреща[а-я]+|расшир[а-я]+|модернизир[а-я]+|отменя[а-я]+|прекраща[а-я]+|утвердил[аи]?|одобрил[аи]?|подписа[а-я]+|продлил[аи]?|смягчил[аи]?|объявил[аи]?|напомнил[аи]?|сообщил[аи]?|предупред[а-я]+|жд[её]т|готовится|действует\s+запрет)", "третье лицо (новость)"),
    (-2, r"пассажирск[а-я]{0,3}\s+(?:перевоз|сообщени|транспорт)|грузов[а-я]{0,3}\s+перевоз|грузоперевоз|международн[а-я]{0,3}\s+перевоз", "транспорт/логистика"),
    (-3, r"в\s+новостях|по\s+данным|сообщается|сообщает\s|напомина[а-я]{0,3}|информиру[а-я]{0,3}|передали\s+(?:в|по|что)|обзор\s+новостей", "новостной стиль"),
]

NEGATIVE.append((-6, r"юрист|адвокат|консультац", "юрист/консультация"))
NEGATIVE.append((-4, r"нужно\s+ли\b|как\s+(?:оформить|получить|заполнить|правильно)|подскажите,?\s+(?:как|где|какие|какой|какая|сколько|почему)|посоветуйте", "вопрос/консультация"))

NEGATIVE.append((-5, r"беру\s+на\s+себя|принимаем\s+заказы|оформлени[а-я]{0,3}\s+(?:документов|виз)", "услуги"))

NEWS_PENALTIES = {"новостной стиль", "третье лицо (новость)"}

# слова, вокруг которых крутятся новости, а не объявления о передаче
BORDER_NEWS_RE = re.compile(
    r"погран|очеред[а-я]{0,3}|таможн|въезд|ввоз|пункт[а-я]{0,3}\s+пропуска|зон[аеы]\s+ожидания|"
    r"рейсов[а-я]{0,3}|автобус[а-я]{0,3}|электронн[а-я]+\s+очеред|расписани|"
    r"правил[а-я]{0,3}\s+(?:въезда|ввоза)|запрет[а-я]{0,3}|ограничени[а-я]{0,3}",
    re.IGNORECASE)

# «Еду Минск-Варшава … Посылки, попутчики» — водитель/путешественник без слов «возьму/могу»:
# в начале сообщения глагол движения, в тексте маршрут из городов с большой буквы, есть предмет.
TRAVEL_VERB_RE = re.compile(r"(?<![а-я])(?:еду|едем|лечу|летим|поеду|полечу|выезжаю|выезжаем|вылетаю|вылетаем)(?![а-я])")
CITY_ROUTE_RE = re.compile(r"[А-ЯЁ][а-яё]{2,}(?:\s*(?:-|–|—|->|→|>)+\s*[А-ЯЁ][а-яё]{2,})+")

# Человек ищет место/машину ДЛЯ СЕБЯ («ищу одно место 14.10», «нужна машина до Варшавы»). Это не водитель
# и не посылка: боту-получателю такое не нужно. «Ищу попутчиков» сюда НЕ входит — так пишут водители.
SEEKER_RE = re.compile(
    r"(?<![а-я])(?:ищу|ищем|нужн[оаы]?|нужен|надо|необходим[оаы]?|требуется|"
    r"хотим\s+найти|хочу\s+найти|помогите\s+найти|подскажите[^.?!\n]{0,40}?нужн[оаы]?)\s+"
    r"(?:(?:\d+(?:\s*-\s*\d+)?|одн[оуа]|двух|два|две|трое|троих|пару|пара)\s*(?:человек\w*\s+)?)?"
    r"(?:мест[оаы]?|попутк[ауи]|машин[ауы]|бус\w*|микроавтобус\w*|водител[а-яь]{0,3}|транспорт)(?![а-я])")
# явные признаки предложения: если они есть, это водитель, а не ищущий место
OFFER_SIGNAL_RE = re.compile(
    r"(?<![а-я])(?:еду|едем|возьму|беру|заберу|везу|везем|выезд[а-я]*|выезжаю|выезжаем|отправление|"
    r"есть\s+(?:\d+\s+)?(?:свободн[а-я]{0,3}\s+)?мест|свободн[а-я]{0,3}\s+мест)(?![а-я])")

# ------------------------------------------------------------------ география

GEO: dict[str, list[str]] = {
    "BY": ["беларус", "минск", "брест", "гродно", "гомель", "витебск", "могилев", "бобруйск", "лида",
           "баранович", "орша", "пинск", "солигорск", "молодечно", "жодино", "слоним",
           "берестовиц", "брузги", "кузниц", "каменный лог", "бенякон", "козлович", "ошмяны",
           "полоцк", "новополоцк", "борисов", "сморгон", "лиозно", "верхнедвинск"],
    "PL": ["польш", "варшав", "краков", "вроцлав", "познан", "лодзь", "гданьск", "катовиц", "белосток",
           "бяла-подляск", "бяла подляск", "люблин", "тереспол", "седльце", "радом", "щецин",
           "ополе", "жешув", "ольштын", "бобровник", "бялоподляск"],
    "LT": ["литв", "вильнюс", "каунас", "клайпед", "висагинас", "шальчининкай", "алитус", "мариямпол",
           "шяуляй", "паневежис", "друскининкай", "лаздия", "эйшишкес"],
    "LV": ["латви", "рига", "даугавпилс", "лиепая", "резекне", "екабпилс", "силене", "патерниеки"],
    "DE": ["германи", "берлин", "мюнхен", "гамбург", "франкфурт", "кельн", "штутгарт", "дортмунд", "дюссельдорф"],
    "RU": ["росси", "москва", "смоленск", "псков", "спб", "санкт-петербург", "калининград"],
}

ORIGIN_RE = re.compile(r"(?:^|[^а-я])(?:из|с|от|со)\s+([а-яёa-z\- ]{3,22})", re.IGNORECASE)
DEST_RE = re.compile(r"(?:^|[^а-я])(?:в|во|до|на|к)\s+([а-яёa-z\- ]{3,22})", re.IGNORECASE)
ROUTE_RE = re.compile(r"([а-яё\-]{3,22})\s*[-–—>]+\s*([а-яё\-]{3,22})", re.IGNORECASE)


def norm(text: str) -> str:
    text = unicodedata.normalize("NFC", text or "").lower().replace("ё", "е")
    return re.sub(r"\s+", " ", text)


def country_of(token: str) -> str | None:
    token = token.strip()
    for code, places in GEO.items():
        for place in places:
            if token.startswith(place) or place.startswith(token):
                return code
    return None


def detect_geo(text: str) -> tuple[list[str], str]:
    """Возвращает (страны, направление вида 'BY->PL'). Направление — эвристика по 'из X' / 'в Y'."""
    flat = norm(text)
    countries: list[str] = []
    for code, places in GEO.items():
        if any(place in flat for place in places):
            countries.append(code)

    origin = dest = None
    for match in ORIGIN_RE.finditer(flat):
        origin = origin or country_of(match.group(1))
    for match in DEST_RE.finditer(flat):
        dest = dest or country_of(match.group(1))
    for match in ROUTE_RE.finditer(flat):            # "Минск-Варшава"
        left, right = country_of(match.group(1)), country_of(match.group(2))
        origin, dest = origin or left, dest or right

    if origin and dest and origin != dest:
        direction = f"{origin}->{dest}"
    elif origin:
        direction = f"{origin}->?"
    elif dest:
        direction = f"?->{dest}"
    else:
        direction = "?"
    return countries, direction


def direction_allowed(direction: str, wanted) -> bool:
    """Подходит ли направление сообщения под список фильтра --only-direction.

    «?» в ФИЛЬТРЕ — любая страна: «?->PL» берёт всё, что едет в Польшу (BY->PL, RU->PL и
    «откуда-то->PL»), «BY->?» — всё, что едет из Беларуси. «?» в НАПРАВЛЕНИИ сообщения — страна
    не определена, такое сообщение подходит только под «?» в фильтре (и под точное «?->PL»).
    Пустой фильтр пропускает всё.
    """
    wanted = [w.strip() for w in (wanted or ()) if w and w.strip()]
    if not wanted:
        return True

    def split(value: str) -> tuple[str, str]:
        if "->" in value:
            left, right = value.split("->", 1)
            return left.strip() or "?", right.strip() or "?"
        return "?", "?"

    origin, dest = split(direction or "?")
    for pattern in wanted:
        if pattern == direction:
            return True
        want_origin, want_dest = split(pattern)
        if ("->" in pattern
                and (want_origin == "?" or want_origin == origin)
                and (want_dest == "?" or want_dest == dest)):
            return True
    return False


# ------------------------------------------------------------------ матчер

@dataclass
class Match:
    text: str
    score: int = 0
    categories: list[str] = field(default_factory=list)
    intents: list[str] = field(default_factory=list)
    hit_labels: list[str] = field(default_factory=list)
    penalties: list[str] = field(default_factory=list)
    countries: list[str] = field(default_factory=list)
    direction: str = "?"
    min_score: int = 4
    explain: bool = False
    details: bool = False
    border_context: bool = False
    seat_seeker: bool = False       # человек ищет место/машину для себя (не водитель и не посылка)

    @property
    def matched(self) -> bool:
        """Совпадение = есть категория, порог взят и при этом есть намерение
        (ищу/предлагаю) либо явный маршрут. Одиночное слово-объект без контекста
        («в Польше подорожали лекарства») ничего не будит."""
        if self.score < self.min_score or not self.categories:
            return False
        if not self.intents:                  # без «везу / нужно передать / кто едет» — не объявление
            return bool(self.details and self.score >= self.min_score + 6
                        and not any(p in NEWS_PENALTIES for p in self.penalties))
        # объявление на фоне новостного контекста (очереди, рейсы, въезд) — планка выше
        if self.border_context and self.score < self.min_score + 6:
            return False
        return True

    @property
    def category(self) -> str:
        if "parcel" in self.categories and "ride" in self.categories:
            return "mixed"
        for preferred in ("parcel", "ride"):
            if preferred in self.categories:
                return preferred
        return self.categories[0] if self.categories else "any"

    @property
    def intent(self) -> str:
        for preferred in ("offer", "request"):
            if preferred in self.intents:
                return preferred
        return "any"

    def as_dict(self) -> dict:
        return {
            "score": self.score, "category": self.category, "intent": self.intent,
            "countries": self.countries, "direction": self.direction,
            "hits": self.hit_labels, "penalties": self.penalties, "details": self.details,
            "seat_seeker": self.seat_seeker,
        }

    def line(self) -> str:
        kind = {"parcel": "📦", "ride": "🚗"}.get(self.category, "•")
        intent = {"offer": "предлагаю", "request": "ищу", "any": ""}.get(self.intent, "")
        head = f"{kind} {intent} [{self.score}] {self.direction}"
        tail = f"    ↳ {','.join(self.hit_labels)}" if self.explain else ""
        return f"{head}  {' '.join(self.text.split())[:160]}{tail}"


def analyze(text: str, min_score: int = 4, explain: bool = False, profile: str = "chat") -> Match:
    """profile='chat' — чат с объявлениями (базовые правила).
    profile='news' — канал новостей: всё, что похоже на пограничную новость, отсекается."""
    flat = norm(text)
    result = Match(text=text, min_score=min_score, explain=explain)
    for category, intent, weight, pattern, label in RULES:
        if re.search(r"(?<![а-яa-z0-9])" + pattern, flat, re.IGNORECASE):
            result.score += weight
            result.hit_labels.append(label)
            if category != "any" and category not in result.categories:
                result.categories.append(category)
            if intent != "any" and intent not in result.intents:
                result.intents.append(intent)
    for penalty, pattern, label in NEGATIVE:
        if re.search(pattern, flat, re.IGNORECASE):
            result.score += penalty
            result.penalties.append(label)

    result.details = "детали объявления" in result.hit_labels
    result.border_context = bool(BORDER_NEWS_RE.search(flat))
    result.seat_seeker = bool(SEEKER_RE.search(flat) and "parcel" not in result.categories
                              and not OFFER_SIGNAL_RE.search(flat))
    result.countries, result.direction = detect_geo(text)
    if result.countries:
        result.score += 1
    if result.direction != "?":
        result.score += 1
    if "?" not in result.direction and result.direction != "?":   # напр. BY->PL
        result.score += 3
    if (not result.intents and result.categories and TRAVEL_VERB_RE.search(flat[:90])
            and CITY_ROUTE_RE.search(text)):
        result.intents.append("offer")
        result.hit_labels.append("еду по маршруту")
        result.score += 3
    # объект + намерение = сильный сигнал
    if result.categories and result.intents:
        result.score += 2
    if profile == "news" and result.border_context:
        result.score = min(result.score, min_score - 1)   # новостной канал: пограничные посты не интересны
    return result


def analyze_many(texts, min_score: int = 4, explain: bool = False, profile: str = "chat") -> list[Match]:
    return [m for m in (analyze(t, min_score, explain, profile) for t in texts) if m.matched]


# ------------------------------------------------------------------ CLI

def main() -> None:
    ap = argparse.ArgumentParser(description="Матчер: посылки/передачи/попутчики")
    ap.add_argument("--text", help="одна строка для проверки")
    ap.add_argument("--file", help=".txt (по строке на сообщение) или .json (список из web_search.py)")
    ap.add_argument("--min-score", type=int, default=4)
    ap.add_argument("--explain", action="store_true", help="показать, какие правила сработали")
    ap.add_argument("--category", help="parcel,mixed,ride")
    ap.add_argument("--only-intent", help="offer,request")
    ap.add_argument("--only-direction", help="BY->PL,PL->BY")
    ap.add_argument("--profile", choices=["chat", "news"], default="chat")
    args = ap.parse_args()

    if args.text:
        match = analyze(args.text, args.min_score, True, args.profile)
        print(match.line())
        print("  matched:", match.matched, "|", json.dumps(match.as_dict(), ensure_ascii=False))
        return

    if not args.file:
        print("Нужен --text или --file. Тест корпуса: python3 selftest_monitor.py", file=sys.stderr)
        sys.exit(2)

    if args.file.endswith(".json"):
        raw = json.loads(open(args.file, encoding="utf-8").read())
        texts = [item.get("text", "") for item in raw]
    else:
        texts = [line.strip() for line in open(args.file, encoding="utf-8") if line.strip()]

    matches = analyze_many(texts, args.min_score, args.explain, args.profile)
    if args.category:
        wanted = {x.strip() for x in args.category.split(",") if x.strip()}
        matches = [m for m in matches if m.category in wanted]
    if args.only_intent:
        wanted = {x.strip() for x in args.only_intent.split(",") if x.strip()}
        matches = [m for m in matches if m.intent in wanted]
    if args.only_direction:
        wanted = {x.strip() for x in args.only_direction.split(",") if x.strip()}
        matches = [m for m in matches if direction_allowed(m.direction, wanted)]
    for match in matches:
        print(match.line())
    print(f"\nсовпало {len(matches)} из {len(texts)} сообщений (min-score={args.min_score})", file=sys.stderr)


if __name__ == "__main__":
    main()
