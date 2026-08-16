import asyncio
import csv
import random as rd
from dataclasses import asdict, dataclass, field
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup, Tag
from fake_useragent import UserAgent

ua = UserAgent()

BASE_DIR = Path(__file__).resolve().parent

START = 0

TIMEOUT = aiohttp.ClientTimeout(total=45)
CONCURRENCY_LIMIT = 6
RETRY_BASE_DELAY = 3

BATCH_SIZE = 1000
BATCH_PAUSE_RANGE = (5, 15)

COMPANIES_FILE = "data/companies.csv"
FAILED_COMPANIES = "data/failed_companies_batches_new.csv"
FAILED_COMPANIES_FILE = "data/failed_companies_batches.csv"

TOKEN, CHAT_ID = "8301946018:AAG67o8YK289r9y3mg835cNVAHW7NhoeCEI", "1000781511"


def get_headers() -> dict:
    return {
        "User-Agent": ua.chrome,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "DNT": "1",
    }


@dataclass
class Filters:
    company_name_words: list[str] = field(default_factory=list)
    titles: list[str] = field(default_factory=list)
    states: list[str] = field(default_factory=list)
    min_found_year: int = 0
    min_revenue: int = 0
    max_revenue: int = 0


@dataclass
class Company:
    url: str
    title: str
    name: str
    year_foundation: str
    state: str
    zip_code: str
    revenue: str
    details: str
    sector: str
    category: str


COMPANY_FIELDNAMES = [
    "id", "title", "name", "year_foundation", "state",
    "zip_code", "url", "revenue", "details", "sector", "category",
]
CSV_DELIMITER = ";"


def _companies_path() -> Path:
    return BASE_DIR / COMPANIES_FILE


def get_last_id() -> int:
    file_path = _companies_path()
    if not file_path.exists():
        return 0
    with open(file_path, "r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f, delimiter=CSV_DELIMITER))

    last_id = 0
    for row in rows:
        try:
            last_id = max(last_id, int(row["id"]))
        except (KeyError, ValueError, TypeError):
            continue
    return last_id


def save_companies(data: list[Company], start_id: int) -> tuple[Path, int]:
    file_path = _companies_path()
    file_path.parent.mkdir(parents=True, exist_ok=True)

    file_has_content = file_path.exists() and file_path.stat().st_size > 0

    current_id = start_id
    rows = []
    for item in data:
        current_id += 1
        row = asdict(item)
        row["id"] = current_id
        rows.append(row)

    mode = "a" if file_has_content else "w"
    with open(file_path, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COMPANY_FIELDNAMES, delimiter=CSV_DELIMITER)
        if not file_has_content:
            writer.writeheader()
        writer.writerows(rows)

    return file_path, current_id


async def send_telegram_message(text: str) -> None:
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    print(f"Ошибка отправки сообщения в Telegram ({resp.status}): {body}")
    except Exception as e:
        print(f"Не удалось отправить сообщение в Telegram: {e}")


async def send_telegram_document(file_path: Path, caption: str = "") -> None:
    if not file_path.exists() or file_path.stat().st_size == 0:
        return

    url = f"https://api.telegram.org/bot{TOKEN}/sendDocument"

    try:
        async with aiohttp.ClientSession() as session:
            with open(file_path, "rb") as f:
                form = aiohttp.FormData()
                form.add_field("chat_id", CHAT_ID)
                if caption:
                    form.add_field("caption", caption)
                form.add_field("document", f, filename=file_path.name)

                async with session.post(url, data=form, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        print(f"Ошибка отправки файла {file_path.name} в Telegram ({resp.status}): {body}")
                    else:
                        print(f"Файл {file_path.name} отправлен в Telegram.")
    except Exception as e:
        print(f"Не удалось отправить файл {file_path.name} в Telegram: {e}")


async def send_results(caption: str) -> None:
    """Отправляет все накопленные файлы результатов в Telegram и удаляет их с диска"""
    files = [
        BASE_DIR / COMPANIES_FILE,
        BASE_DIR / FAILED_COMPANIES_FILE,
    ]

    await send_telegram_message(caption)

    for file_path in files:
        if file_path.exists() and file_path.stat().st_size > 0:
            await send_telegram_document(file_path, caption=file_path.name)


class AllProxiesExhaustedError(Exception):
    """Все прокси из пула оказались нерабочими"""


class ProxyRotator:
    def __init__(self, proxies: list[str], shuffle: bool = True):
        if not proxies:
            raise ValueError("Список прокси пуст")

        self.proxies = [self._normalize_proxy(p) for p in proxies]
        if shuffle:
            rd.shuffle(self.proxies)

        self.index = 0
        self.bad = set()
        self.lock = asyncio.Lock()

    @staticmethod
    def _normalize_proxy(proxy: str) -> str:
        if "://" in proxy:
            return proxy

        parts = proxy.split(":")
        if len(parts) != 4:
            raise ValueError(
                f"Не удалось распознать формат прокси: {proxy!r}. "
                f"Ожидается 'ip:port:user:pass' или уже готовый URL со схемой."
            )

        ip, port, user, password = parts
        return f"http://{user}:{password}@{ip}:{port}"

    @property
    def current(self) -> str:
        return self.proxies[self.index]

    async def rotate(self, mark_bad: str | None = None) -> str:
        async with self.lock:
            if mark_bad:
                self.bad.add(mark_bad)

            if len(self.bad) >= len(self.proxies):
                raise AllProxiesExhaustedError("Все прокси из пула исчерпаны")

            for _ in range(len(self.proxies)):
                self.index = (self.index + 1) % len(self.proxies)
                if self.current not in self.bad:
                    break

            print(f"Переключились на прокси: {self.current}")
            return self.current

    def reset_bad(self) -> None:
        self.bad.clear()


class ProxyRequester:
    BLOCK_MARKERS = ("has been blocked", "likely blocked")

    def __init__(
            self,
            session: aiohttp.ClientSession,
            rotator: ProxyRotator,
            max_retries: int = 3,
            semaphore: asyncio.Semaphore | None = None,
    ):
        self.session = session
        self.rotator = rotator
        self.max_retries = max_retries
        self.semaphore = semaphore or asyncio.Semaphore(CONCURRENCY_LIMIT)

    async def get_text(self, url: str) -> tuple[int, str]:
        last_exc = None

        for attempt in range(1, self.max_retries + 1):
            proxy = self.rotator.current
            request_headers = {"User-Agent": ua.chrome} if attempt > 1 else None

            try:
                async with self.semaphore:
                    async with self.session.get(url, proxy=proxy, headers=request_headers) as r:
                        status = r.status
                        text = await r.text()

                if self._looks_blocked(text):
                    raise aiohttp.ClientError(f"IP заблокирован на прокси {proxy}")

                return status, text
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_exc = exc

                print(f"Проблема с прокси {proxy}, пробуем другой...")

                try:
                    await self.rotator.rotate(mark_bad=proxy)
                except AllProxiesExhaustedError:
                    raise

                if attempt < self.max_retries:
                    delay = RETRY_BASE_DELAY * attempt
                    print(f"Пауза {delay} сек. перед следующей попыткой...")
                    await asyncio.sleep(delay)

        raise RuntimeError(f"Не удалось выполнить запрос {url} за {self.max_retries} попыток") from last_exc

    @classmethod
    def _looks_blocked(cls, text: str) -> bool:
        lowered = text.lower()
        return any(marker in lowered for marker in cls.BLOCK_MARKERS)


def _text_or_empty(node: Tag | None) -> str:
    return node.get_text(strip=True) if node else ""


def _table_value(soup: BeautifulSoup, label: str) -> str:
    for td in soup.select("td.my-table-td-header"):
        header_text = td.get_text(strip=True).rstrip(":")
        if header_text.lower() != label.lower():
            continue

        value_td = td.find_next_sibling("td")
        if value_td is None:
            return ""

        parts = [a.get_text(strip=True) for a in value_td.find_all("a")]
        if parts:
            return ", ".join(parts)

        return value_td.get_text(" ", strip=True)

    return ""


def _info_box_value(soup: BeautifulSoup, label: str) -> str:
    for header in soup.select("div.company-info-header"):
        header_text = header.get_text(strip=True).rstrip(":")
        if header_text.lower() != label.lower():
            continue

        content = header.find_next_sibling("div", class_="company-info-content")
        if content is None:
            return ""

        return content.get_text(" ", strip=True)

    return ""


async def _parse_company_page(
        requester: ProxyRequester,
        url: str,
        sector: str,
        category: str,
) -> Company | None:
    status, text = await requester.get_text(url)

    soup = BeautifulSoup(text, "lxml")

    name = _text_or_empty(soup.select_one("span.company-name[itemprop='name']"))
    if not name:
        name = _text_or_empty(soup.select_one("span[itemprop='name']"))

    street = _text_or_empty(soup.select_one("span[itemprop='streetAddress']"))
    city = _text_or_empty(soup.select_one("span[itemprop='addressLocality']"))
    region = _text_or_empty(soup.select_one("span[itemprop='addressRegion']"))
    zip_code = _text_or_empty(soup.select_one("span[itemprop='postalCode']"))
    contact = _text_or_empty(soup.select_one("span[itemprop='employee']"))
    title = _text_or_empty(soup.select_one("span[itemprop='contactType']"))
    phone = _text_or_empty(soup.select_one("span[itemprop='telephone']"))
    website = _text_or_empty(soup.select_one("span[itemprop='url']"))

    year_founded = ""
    for header in soup.select("div.company-info-header"):
        if "year founded" in header.get_text(strip=True).lower():
            span = header.find("span")
            if span:
                year_founded = span.get_text(strip=True)

    revenue = _info_box_value(soup, "Revenue")

    detail_fields = {
        "name": name,
        "street": street,
        "city": city,
        "region": region,
        "zip_code": zip_code,
        "contact": contact,
        "title": title,
        "phone": phone,
        "website": website,
        "Sector": _table_value(soup, "Sector"),
        "Category": _table_value(soup, "Category"),
        "Industry": _table_value(soup, "Industry"),
        "SIC Code": _table_value(soup, "SIC Code"),
        "NAICS Name": _table_value(soup, "NAICS Name"),
        "NAICS Code": _table_value(soup, "NAICS Code"),
        "Location Type": _info_box_value(soup, "Location Type"),
        "Revenue": revenue,
        "Employees Here": _info_box_value(soup, "Employees Here"),
        "Facility Size": _info_box_value(soup, "Facility Size"),
        "Year Founded": year_founded,
    }

    details = " | ".join(f"{key}: {value}" for key, value in detail_fields.items())

    return Company(
        url=url,
        title=title,
        name=name,
        year_foundation=year_founded,
        state=region,
        zip_code=zip_code,
        revenue=revenue,
        details=details,
        sector=sector,
        category=category,
    )


def _company_passes_filters(company: Company, filters: Filters) -> bool:
    if filters.titles and company.title not in filters.titles:
        return False

    if filters.min_found_year != 0:
        try:
            year = int(company.year_foundation)
        except (TypeError, ValueError):
            return False
        if year < filters.min_found_year:
            return False

    return True


async def get_companies_data(
        rotator: ProxyRotator,
        company_links: list[tuple[str, str, str]],
        filters: Filters,
        semaphore: asyncio.Semaphore,
) -> list[Company]:
    print(f"Обработка страниц компаний: {len(company_links)}")

    async with aiohttp.ClientSession(headers=get_headers(), timeout=TIMEOUT) as session:
        requester = ProxyRequester(session, rotator, semaphore=semaphore)

        tasks = [
            _parse_company_page(requester, url, sector, category)
            for sector, category, url in company_links
        ]
        results = await asyncio.gather(*tasks)

    companies = [c for c in results if c]
    filtered = [c for c in companies if _company_passes_filters(c, filters)]

    print(f"Загружено страниц: {len(companies)}, прошло фильтры: {len(filtered)}")
    return filtered


def load_hierarchy() -> list[tuple[str, str, str]]:
    file_path = BASE_DIR / FAILED_COMPANIES
    if not file_path.exists():
        return []

    rows = []
    with open(file_path, "r", newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            sector = (row.get("Sector") or "").strip()
            category = (row.get("Category") or "").strip()
            url = (row.get("Industry URL") or "").strip()
            if url:
                rows.append((sector, category, url))
    return rows


def _chunked(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _append_failed_rows(filename: str, header: list[str], rows: list[tuple[str, str, str]]) -> None:
    if not rows:
        return

    file_path = BASE_DIR / filename
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_has_content = file_path.exists() and file_path.stat().st_size > 0

    with open(file_path, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        if not file_has_content:
            writer.writerow(header)
        writer.writerows(rows)


def load_failed_companies_link():
    file_path = BASE_DIR / FAILED_COMPANIES
    if not file_path.exists():
        return []

    rows = []
    with open(file_path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            sector = (row.get("Sector") or "").strip()
            category = (row.get("Category") or "").strip()
            url = (row.get("Company URL") or "").strip()
            if url:
                rows.append((sector, category, url))

    print(f"Прочитано {len(rows)} строк из {file_path}")
    return rows


def log_failed_companies_batch(company_links: list[tuple[str, str, str]]) -> None:
    rows = [(sector, category, url) for url, sector, category in company_links]
    print(f"Сохранено компаний: {rows}")
    _append_failed_rows(FAILED_COMPANIES_FILE, ["Sector", "Category", "Company URL"], rows)


async def run() -> None:
    failed_companies_links = load_failed_companies_link()
    if not failed_companies_links:
        print(f"Файл {FAILED_COMPANIES} не найден или пуст.")
        await send_telegram_message(f"⚠️ Парсер buzzfile: файл {FAILED_COMPANIES} не найден или пуст.")
        return

    batches = list(_chunked(failed_companies_links, BATCH_SIZE))
    print(
        f"Загружено {len(failed_companies_links)} Company URL, разбито на {len(batches)} батчей по {BATCH_SIZE}."
    )

    proxies = [
        "34.132.128.99:62929:236843:236843",
        "173.201.38.181:24116:236843:236843"
    ]

    rotator = ProxyRotator(proxies)
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    filters = Filters()

    for file_path in [BASE_DIR / COMPANIES_FILE, BASE_DIR / FAILED_COMPANIES_FILE]:
        if file_path.exists() and file_path.stat().st_size > 0:
            try:
                file_path.unlink()
                print(f"Файл {file_path} удалён с диска")
            except OSError as e:
                print(f"Не удалось удалить файл {file_path}: {e}")

    last_id = get_last_id()
    total_saved = 0

    start = START
    if start >= len(batches):
        print("START указывает за пределы списка батчей — нечего обрабатывать.")
        await send_telegram_message("✅ Парсер buzzfile: START за пределами списка батчей, обрабатывать нечего.")
        return

    print(f"Начинаем с батча {start + 1} из {len(batches)} (индекс {start}).")

    last_completed_batch = start - 1

    for batch_num, batch_rows in enumerate(batches[start:], start=start):
        print(f"\n==== Батч {batch_num + 1}/{len(batches)} (индекс {batch_num}, {len(batch_rows)} Industry URL) ====")
        rotator.reset_bad()

        try:
            companies_data = await get_companies_data(rotator, batch_rows, filters, semaphore)
        except AllProxiesExhaustedError:
            print(f"Батч {batch_num + 1}: все прокси исчерпаны на обработке компаний.")
            log_failed_companies_batch(batch_rows)
            await send_results(
                f"🛑 Прокси исчерпаны на батче {batch_num + 1}/{len(batches)} (обработка компаний).\n"
                f"Сохранено компаний за этот запуск: {total_saved}.\n"
                f"Последний полностью обработанный батч: {last_completed_batch + 1}.\n"
                f"Чтобы продолжить основной парсинг: "
                f"замените прокси, укажите START = {batch_num + 1} и запустите заново.\n"
                f"Необработанный батч уже сохранён в {FAILED_COMPANIES_FILE} "
                f"и может быть обработан отдельным скриптом."
            )
            return
        except Exception as e:
            print(
                f"Батч {batch_num + 1}: ошибка обработки компаний ({e}). Записал в {FAILED_COMPANIES_FILE}, иду дальше."
            )
            log_failed_companies_batch(batch_rows)
            last_completed_batch = batch_num
            continue

        if companies_data:
            file_path, last_id = save_companies(companies_data, start_id=last_id)
            total_saved += len(companies_data)
            print(f"Батч {batch_num}: сохранено {len(companies_data)} компаний (всего за прогон: {total_saved}).")
        else:
            print(f"Батч {batch_num}: компаний не найдено.")

        last_completed_batch = batch_num

        if batch_num != len(batches) - 1:
            pause = rd.uniform(*BATCH_PAUSE_RANGE)
            print(f"Пауза перед следующим батчем: {pause:.0f} сек.")
            await asyncio.sleep(pause)

    msg = (
        f"✅ Парсинг buzzfile полностью завершён!\n"
        f"Обработано батчей: {len(batches)}.\n"
        f"Сохранено компаний за этот запуск: {total_saved}."
    )
    print(msg)
    await send_results(msg)


if __name__ == "__main__":
    main()
