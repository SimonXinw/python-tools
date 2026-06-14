import calendar
from datetime import date

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_utils import launch_browser, wait_for_manual_login
import os
import time
import random
import csv


SCHOOL_NAME_SELECTOR = "div.school-tab_name__3pOZK"
PAGE_LOADED_SELECTOR = ".main-nav_logo2__bmYaw"
HOME_URL = "https://www.gaokao.cn"
CSV_HEADER = ["院校名称", "代码", "状态", "错误信息", "日期"]
CSV_READ_ENCODINGS = ("utf-8-sig", "utf-8", "gbk", "gb18030")
CSV_WRITE_ENCODING = "utf-8-sig"
DONE_STATUSES = frozenset({"成功", "无效"})
FRESH_MONTHS = 6

# 是否按「日期」跳过：True=半年内成功/无效跳过；False=只看状态，成功/无效一律跳过（不校验日期）
ENABLE_DATE_SKIP = False


def read_csv_rows(file_path):
    last_err = None
    for encoding in CSV_READ_ENCODINGS:
        try:
            with open(file_path, newline="", encoding=encoding) as csvfile:
                return list(csv.reader(csvfile))
        except UnicodeDecodeError as err:
            last_err = err
    raise last_err


class UniversityScraper:
    def __init__(
        self,
        num_urls,
        save_csv_path,
        base_dir,
    ):
        self.num_urls = num_urls
        self.page = None
        self.context = None
        self.playwright = None
        self.records = {}
        self.save_csv_path = save_csv_path
        self.base_dir = base_dir

        self._load_existing_results()

    def _normalize_row(self, row):
        name = row[0] if len(row) > 0 else ""
        code = str(row[1]).strip() if len(row) > 1 else ""
        status = row[2] if len(row) > 2 else ""
        error = row[3] if len(row) > 3 else ""
        record_date = row[4] if len(row) > 4 else ""
        return [name, code, status, error, record_date]

    def _today_str(self):
        return date.today().strftime("%Y-%m-%d")

    def _build_record(self, name, code, status, error=""):
        return [name, code, status, error, self._today_str()]

    def _parse_record_date(self, record_date_str):
        if not record_date_str:
            return None

        try:
            return date.fromisoformat(str(record_date_str).strip())
        except ValueError:
            return None

    def _half_year_ago(self):
        today = date.today()
        year = today.year
        month = today.month - FRESH_MONTHS

        if month <= 0:
            month += 12
            year -= 1

        max_day = calendar.monthrange(year, month)[1]
        day = min(today.day, max_day)
        return date(year, month, day)

    def _is_fresh_done(self, record):
        if not record or record[2] not in DONE_STATUSES:
            return False

        if not ENABLE_DATE_SKIP:
            return True

        record_date = self._parse_record_date(record[4] if len(record) > 4 else "")
        if not record_date:
            return False

        return record_date >= self._half_year_ago()

    def _load_rows_into_records(self, rows):
        if not rows:
            return

        start_index = 1 if rows[0] and rows[0][0] == "院校名称" else 0

        for row in rows[start_index:]:
            if len(row) < 3:
                continue

            normalized = self._normalize_row(row)
            code = normalized[1]
            if code:
                self.records[code] = normalized

    def _load_existing_results(self):
        if os.path.exists(self.save_csv_path):
            self._load_rows_into_records(read_csv_rows(self.save_csv_path))
        else:
            print("未找到已存在的结果 CSV，从头开始")

        pending_count = sum(
            1 for school_id in range(1, self.num_urls + 1) if self._should_process(school_id)
        )
        done_count = sum(
            1
            for school_id in range(1, self.num_urls + 1)
            if self._is_fresh_done(self.records.get(str(school_id)))
        )

        skip_hint = "超过半年数据重拉" if ENABLE_DATE_SKIP else "不校验日期"
        done_hint = "半年内已完成" if ENABLE_DATE_SKIP else "已完成（不校验日期）"

        print(
            f"已加载 {len(self.records)} 条记录，"
            f"待处理 {pending_count} 条（含失败重试、{skip_hint}），"
            f"{done_hint} {done_count} 条（成功 + 无效）"
        )

    def _should_process(self, school_id):
        record = self.records.get(str(school_id))
        if record is None:
            return True
        return not self._is_fresh_done(record)

    def _init_browser(self):
        self.playwright = sync_playwright().start()
        self.context, self.page = launch_browser(self.playwright, self.base_dir)
        wait_for_manual_login(self.page)

    def _normalize_url(self, url):
        return url.split("?")[0].rstrip("/")

    def _is_homepage(self, url):
        return self._normalize_url(url) == HOME_URL

    def _log_fail(self, school_code, reason):
        print(f"[{school_code}] 失败：{reason}")

    def _save_record(self, row):
        self.records[row[1]] = row
        self._flush_csv()

    def _flush_csv(self):
        with open(
            self.save_csv_path, "w", newline="", encoding=CSV_WRITE_ENCODING
        ) as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(CSV_HEADER)

            for school_id in range(1, self.num_urls + 1):
                record = self.records.get(str(school_id))
                if record:
                    writer.writerow(record)

    def _throttle(self):
        time.sleep(random.uniform(0.01, 0.32))

    def _wait_for_page_loaded(self, timeout_ms=1600):
        self.page.wait_for_selector(PAGE_LOADED_SELECTOR, timeout=timeout_ms)

    def _resolve_school_page(self, url):
        """导航 logo 出现即视为页面加载完成，再判断首页/校名/无效。"""
        self._wait_for_page_loaded()

        current_url = self.page.url

        if self._is_homepage(current_url):
            return "invalid", "ID不存在"

        if self._normalize_url(current_url) != self._normalize_url(url):
            return "failed", "页面跳转失败"

        try:
            self.page.wait_for_selector(SCHOOL_NAME_SELECTOR, timeout=1500)
            school_name = self.page.locator(SCHOOL_NAME_SELECTOR).first.inner_text().strip()
            if school_name:
                return "success", school_name
        except PlaywrightTimeoutError:
            pass

        return "invalid", "ID不存在"

    def _process_url(self, school_id):
        school_code = str(school_id)
        url = f"https://www.gaokao.cn/school/{school_id}"
        need_throttle = True

        try:
            self.page.goto(url, wait_until="domcontentloaded", timeout=3000)
            status, payload = self._resolve_school_page(url)

            if status == "invalid":
                self._save_record(self._build_record("", school_code, "无效", payload))
                print(f"[{school_id}/{self.num_urls}] {school_code} 无效（ID无院校）")
                need_throttle = False
                return

            if status == "success":
                self._save_record(self._build_record(payload, school_code, "成功"))
                print(f"[{school_id}/{self.num_urls}] {school_code} {payload}")
                return

            self._save_record(self._build_record("", school_code, "失败", payload))
            self._log_fail(school_code, payload)

        except PlaywrightTimeoutError:
            self._save_record(self._build_record("", school_code, "失败", "页面加载超时"))
            self._log_fail(school_code, "页面加载超时")
        except Exception as e:
            error_msg = str(e).split("\n")[0]
            self._save_record(self._build_record("", school_code, "失败", error_msg))
            self._log_fail(school_code, error_msg)
        finally:
            if need_throttle:
                self._throttle()

    def scrape(self):
        self._init_browser()

        try:
            for school_id in range(1, self.num_urls + 1):
                if not self._should_process(school_id):
                    continue

                self._process_url(school_id)
        finally:
            if self.context:
                self.context.close()
            if self.playwright:
                self.playwright.stop()

            self._flush_csv()


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    num_urls = 4000
    save_csv_path = os.path.join(current_dir, "掌上高考-id_院校_map表.csv")

    scraper = UniversityScraper(
        num_urls,
        save_csv_path,
        current_dir,
    )
    scraper.scrape()
