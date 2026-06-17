import os
import time
import asyncio
import random
import csv
import threading
from urllib.parse import quote

import pandas as pd
from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
)

# 并发开关：True 时启用多标签页并发；False 时单标签页顺序执行
ENABLE_CONCURRENT = True
CONCURRENT_WORKERS = 16

# 状态表落盘：内存更新后由后台定时批量写入，降低 Windows 下 os.replace 冲突
STATUS_FLUSH_INTERVAL_SEC = 1.0
FILE_WRITE_MAX_RETRIES = 5
FILE_WRITE_RETRY_DELAY_SEC = 0.3

# 任务间隔与导航前短暂抖动，降低瞬时并发峰值
NAVIGATE_DELAY_SEC = (0.02, 0.06)
WORKER_TASK_DELAY_SEC = (0.05, 0.15)

# 页面 title 模块抓取字段（.university-tags-pc-top 的 span[1..3]）
HEADER_INFO_FIELDS = ("层次", "类型", "性质")

# 页面院校徽标标签（.university-tags-pc 内 span 文本匹配）
BADGE_TAG_FIELDS = ("985", "211", "双一流")
BADGE_TAG_TEXTS = frozenset(BADGE_TAG_FIELDS)

CSV_READ_ENCODINGS = ("utf-8-sig", "utf-8", "gbk", "gb18030")
CSV_OUTPUT_ENCODING = "utf-8-sig"

RESULT_COLUMNS = [
    "序号",
    "学校名称",
    "主管部门",
    "省份",
    "城市",
    "985",
    "211",
    "双一流",
    "类型",
    "层次",
    "性质",
    "状态",
]

SKIP_STATUSES = {"成功", "无效院校"}
INVALID_SCHOOL_STATUS = "无效院校"


def read_csv_with_encodings(file_path):
    last_err = None
    for encoding in CSV_READ_ENCODINGS:
        try:
            return pd.read_csv(file_path, encoding=encoding)
        except UnicodeDecodeError as err:
            last_err = err
    raise last_err


def _decode_csv_line_bytes(line_bytes):
    for encoding in ("utf-8-sig", "utf-8", "gbk", "gb18030"):
        try:
            return line_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
    return line_bytes.decode("utf-8", errors="replace")


def _read_mixed_encoding_csv_rows(file_path):
    with open(file_path, "rb") as file_obj:
        raw = file_obj.read()
    lines = [line for line in raw.splitlines() if line.strip()]
    return [_decode_csv_line_bytes(line) for line in lines]


def _is_valid_utf8_sig_text_file(file_path):
    try:
        with open(file_path, "rb") as file_obj:
            file_obj.read().decode(CSV_OUTPUT_ENCODING)
        return True
    except UnicodeDecodeError:
        return False


class QuarkBasicInfoScraper:
    def __init__(
        self,
        school_source_path,
        status_save_path,
        result_path=None,
        enable_concurrent=ENABLE_CONCURRENT,
        concurrent_workers=CONCURRENT_WORKERS,
    ):
        self.school_source_path = school_source_path
        self.status_save_path = status_save_path
        if result_path is None:
            output_dir = os.path.dirname(os.path.abspath(school_source_path))
            result_path = os.path.join(output_dir, "普通高校-基本信息.csv")
        self.result_path = result_path
        self.enable_concurrent = enable_concurrent
        self.concurrent_workers = max(1, concurrent_workers if enable_concurrent else 1)

        self.df = None
        self._save_lock = threading.Lock()
        self._status_dirty = False
        self._progress_lock = threading.Lock()
        self._total_schools = 0
        self._total_pending = 0
        self._skip_count = 0
        self._started_count = 0
        self._finished_count = 0

    def _read_source_table(self, file_path):
        if file_path.endswith(".csv"):
            return read_csv_with_encodings(file_path)
        if file_path.endswith(".xlsx"):
            return pd.read_excel(file_path, engine="openpyxl")
        return pd.read_excel(file_path)

    def _atomic_replace_file(self, file_path, write_fn, label="文件"):
        temp_path = f"{file_path}.tmp"
        last_err = None

        for attempt in range(FILE_WRITE_MAX_RETRIES):
            try:
                write_fn(temp_path)
                os.replace(temp_path, file_path)
                return
            except PermissionError as err:
                last_err = err
                if attempt < FILE_WRITE_MAX_RETRIES - 1:
                    delay = FILE_WRITE_RETRY_DELAY_SEC * (attempt + 1)
                    print(
                        f"【警告】{label}写入被占用，"
                        f"{delay:.1f}s 后重试 ({attempt + 1}/{FILE_WRITE_MAX_RETRIES})"
                    )
                    time.sleep(delay)
            finally:
                if os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except OSError:
                        pass

        raise last_err

    def _save_status_dataframe(self):
        def write_status(temp_path):
            self.df.to_csv(temp_path, index=False, encoding=CSV_OUTPUT_ENCODING)

        self._atomic_replace_file(
            self.status_save_path, write_status, label="状态表"
        )

    def _flush_status_to_disk(self):
        with self._save_lock:
            if not self._status_dirty:
                return False
            self._save_status_dataframe()
            self._status_dirty = False
            return True

    async def _status_flush_loop(self, stop_event):
        while True:
            try:
                self._flush_status_to_disk()
            except PermissionError as err:
                with self._save_lock:
                    self._status_dirty = True
                print(f"【警告】状态表落盘失败，将在下次定时重试: {err}")

            if stop_event.is_set():
                break
            await asyncio.sleep(STATUS_FLUSH_INTERVAL_SEC)

        try:
            self._flush_status_to_disk()
        except PermissionError as err:
            with self._save_lock:
                self._status_dirty = True
            print(f"【警告】退出前状态表落盘失败: {err}")

    def _migrate_legacy_xlsx_status(self):
        legacy_xlsx = f"{os.path.splitext(self.status_save_path)[0]}.xlsx"
        if not os.path.exists(legacy_xlsx):
            return False

        try:
            status_df = pd.read_excel(legacy_xlsx, engine="openpyxl")
        except Exception as err:
            backup_path = f"{legacy_xlsx}.corrupt.bak"
            if os.path.exists(backup_path):
                os.remove(backup_path)
            os.replace(legacy_xlsx, backup_path)
            print(
                f"【警告】旧 xlsx 状态表已损坏，已备份为 {os.path.basename(backup_path)}"
                f"（原因: {err}）"
            )
            return False

        temp_path = f"{self.status_save_path}.tmp"
        status_df.to_csv(temp_path, index=False, encoding=CSV_OUTPUT_ENCODING)
        os.replace(temp_path, self.status_save_path)
        backup_path = f"{legacy_xlsx}.bak"
        if os.path.exists(backup_path):
            os.remove(backup_path)
        os.replace(legacy_xlsx, backup_path)
        print(
            f"【提示】已将旧 xlsx 状态表迁移为 csv，保留 {len(status_df)} 行，"
            f"旧文件备份为 {os.path.basename(backup_path)}"
        )
        return True

    def _load_status_dataframe(self):
        if not os.path.exists(self.status_save_path):
            if self._migrate_legacy_xlsx_status():
                return read_csv_with_encodings(self.status_save_path)
            return self._read_source_table(self.school_source_path)

        try:
            return read_csv_with_encodings(self.status_save_path)
        except Exception as err:
            backup_path = f"{self.status_save_path}.corrupt.bak"
            if os.path.exists(backup_path):
                os.remove(backup_path)
            os.replace(self.status_save_path, backup_path)
            print(
                f"【警告】状态文件已损坏，已备份为 {os.path.basename(backup_path)}，"
                f"将从源表重新初始化（原因: {err}）"
            )
            if self._migrate_legacy_xlsx_status():
                return read_csv_with_encodings(self.status_save_path)
            return self._read_source_table(self.school_source_path)

    def _init_source_excel(self):
        self.df = self._load_status_dataframe()
        if "状态" not in self.df.columns:
            self.df["状态"] = ""
        self.df["状态"] = self.df["状态"].fillna("").astype(str)

    @staticmethod
    def _normalize_cell_value(value):
        if pd.isna(value):
            return ""
        if isinstance(value, float) and value == int(value):
            text = str(int(value))
        else:
            text = str(value).strip()
        if text.lower() == "nan":
            return ""
        if text.endswith(".0") and text[:-2].isdigit():
            text = text[:-2]
        return text

    def _build_result_row(
        self,
        row_index,
        header_info=None,
        badge_info=None,
        page_school_name="",
        has_former_name=False,
    ):
        row = self.df.loc[row_index]
        result = {}
        for column_name in RESULT_COLUMNS:
            if column_name == "状态":
                result[column_name] = ""
                continue
            result[column_name] = self._normalize_cell_value(row.get(column_name, ""))

        # 层次/类型/性质：页面抓取优先，未抓到则沿用源表原值
        for field_name in HEADER_INFO_FIELDS:
            scraped_value = str((header_info or {}).get(field_name, "")).strip()
            if scraped_value:
                result[field_name] = scraped_value

        # 985/211/双一流：页面出现对应标签则写入，未出现则沿用源表原值
        for field_name in BADGE_TAG_FIELDS:
            if field_name in (badge_info or {}):
                result[field_name] = field_name

        # 页面出现曾用名时，学校名称更新为页面最新名称
        if has_former_name and page_school_name:
            result["学校名称"] = page_school_name

        return [result[column_name] for column_name in RESULT_COLUMNS]

    def _create_empty_result_file(self):
        with open(
            self.result_path, mode="w", encoding=CSV_OUTPUT_ENCODING, newline=""
        ) as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(RESULT_COLUMNS)

    def _repair_result_file_encoding(self):
        if not os.path.exists(self.result_path):
            return False

        if _is_valid_utf8_sig_text_file(self.result_path):
            return False

        backup_path = f"{self.result_path}.mixed-encoding.bak"
        if os.path.exists(backup_path):
            os.remove(backup_path)
        os.replace(self.result_path, backup_path)

        text_rows = _read_mixed_encoding_csv_rows(backup_path)
        if not text_rows:
            self._create_empty_result_file()
            print(
                f"【警告】结果表编码异常且为空，已重建；"
                f"原文件备份为 {os.path.basename(backup_path)}"
            )
            return True

        parsed_rows = list(csv.reader(text_rows))
        row_count = max(0, len(parsed_rows) - 1)

        def write_result(temp_path):
            with open(
                temp_path, mode="w", encoding=CSV_OUTPUT_ENCODING, newline=""
            ) as f:
                writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
                writer.writerows(parsed_rows)

        self._atomic_replace_file(
            self.result_path, write_result, label="结果表"
        )
        print(
            f"【警告】结果表存在混编（常见：表头 GBK + 追加行 UTF-8），"
            f"已整表重写为 {CSV_OUTPUT_ENCODING}，保留 {row_count} 行；"
            f"原文件备份为 {os.path.basename(backup_path)}"
        )
        return True

    def _read_result_dataframe(self, file_path):
        try:
            return read_csv_with_encodings(file_path)
        except Exception as err:
            backup_path = f"{file_path}.corrupt.bak"
            if os.path.exists(backup_path):
                os.remove(backup_path)
            os.replace(file_path, backup_path)
            print(
                f"【警告】结果文件已损坏，已备份为 {os.path.basename(backup_path)}，"
                f"将重建空表（原因: {err}）"
            )
            return pd.DataFrame(columns=RESULT_COLUMNS)

    def _init_result_file(self):
        if not os.path.exists(self.result_path):
            self._create_empty_result_file()
            print("【提示】结果表不存在，已创建空 csv，后续仅追加写入")
            return

        self._repair_result_file_encoding()

        existing_df = self._read_result_dataframe(self.result_path)
        row_count = len(existing_df)

        if list(existing_df.columns) == RESULT_COLUMNS:
            print(f"【提示】结果表已存在 {row_count} 行，后续仅追加写入")
            return

        migrated_df = pd.DataFrame(index=existing_df.index)
        for column_name in RESULT_COLUMNS:
            if column_name in existing_df.columns:
                migrated_df[column_name] = existing_df[column_name]
            else:
                migrated_df[column_name] = ""

        def write_result(temp_path):
            migrated_df.to_csv(temp_path, index=False, encoding=CSV_OUTPUT_ENCODING)

        self._atomic_replace_file(
            self.result_path, write_result, label="结果表"
        )
        print(
            f"【提示】结果表表头已对齐，保留 {len(migrated_df)} 行，"
            f"后续仅追加写入"
        )

    def _mark_task_started(self, school_name, worker_id):
        with self._progress_lock:
            self._started_count += 1
            traversed = self._skip_count + self._started_count
        print(
            f"[W{worker_id}] [{traversed}/{self._total_schools}] "
            f"开始 | {school_name}"
        )
        return traversed

    def _mark_task_finished(self, school_name, worker_id, result_text):
        with self._progress_lock:
            self._finished_count += 1
            traversed = self._skip_count + self._finished_count
            remaining = self._total_pending - self._finished_count
        print(
            f"[W{worker_id}] [{traversed}/{self._total_schools}] "
            f"完成(剩{remaining}) | {school_name} | {result_text}"
        )

    def _build_url(self, school_name):
        encoded_school = quote(school_name)
        return (
            "https://vt.quark.cn/blm/gaokao-college-794/tab"
            f"?university_name={encoded_school}"
        )

    def _save_status(self, row_index, status):
        with self._save_lock:
            self.df.at[row_index, "状态"] = status
            self._status_dirty = True

    def _write_result_rows(self, rows_data):
        """追加结果行（调用方需已持有 _save_lock）。"""
        if not rows_data:
            return

        last_err = None
        for attempt in range(FILE_WRITE_MAX_RETRIES):
            try:
                with open(
                    self.result_path,
                    mode="a",
                    encoding=CSV_OUTPUT_ENCODING,
                    newline="",
                ) as f:
                    writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
                    writer.writerows(rows_data)
                return
            except PermissionError as err:
                last_err = err
                if attempt < FILE_WRITE_MAX_RETRIES - 1:
                    delay = FILE_WRITE_RETRY_DELAY_SEC * (attempt + 1)
                    print(
                        f"【警告】结果表追加被占用，"
                        f"{delay:.1f}s 后重试 ({attempt + 1}/{FILE_WRITE_MAX_RETRIES})"
                    )
                    time.sleep(delay)
        raise last_err

    def _commit_scrape_success(self, row_index, result_row):
        """先写结果表、再标成功，同一把锁内完成，保证断点续传一致。"""
        with self._save_lock:
            self._write_result_rows([result_row])
            self.df.at[row_index, "状态"] = "成功"
            self._status_dirty = True

    async def _get_page_school_name(self, page, timeout=5000):
        name_locator = page.locator(".university-logo-left .qk-title-text em")
        fallback_locator = page.locator(
            ".university-logo-left .qk-title-text"
        ).first

        try:
            await name_locator.first.wait_for(state="visible", timeout=timeout)
            return (await name_locator.first.inner_text()).strip()
        except PlaywrightTimeoutError:
            pass

        try:
            await fallback_locator.wait_for(state="visible", timeout=timeout)
        except PlaywrightTimeoutError:
            return None

        return (await fallback_locator.inner_text()).strip()

    async def _get_page_school_header_info(self, page, timeout=5000):
        """
        从 title 模块 .university-tags-pc-top 抓取院校标签。
        无曾用名：span[0]=省份, span[1]=层次, span[2]=类型, span[3]=性质
        有曾用名：span[0]=曾用名, span[1]=省份, span[2]=层次, span[3]=类型, span[4]=性质
        返回 (header_info, has_former_name)。
        """
        tags_locator = page.locator(".university-tags-pc-top span")
        try:
            await tags_locator.first.wait_for(state="visible", timeout=timeout)
        except PlaywrightTimeoutError:
            return {}, False

        span_count = await tags_locator.count()
        if span_count < 2:
            return {}, False

        former_name_locator = page.locator(".university-tags-pc-top-cengyongming")
        has_former_name = await former_name_locator.count() > 0
        if not has_former_name:
            first_text = (await tags_locator.first.inner_text()).strip()
            has_former_name = first_text.startswith("曾用名")

        start_index = 2 if has_former_name else 1
        header_info = {}

        for offset, field_name in enumerate(HEADER_INFO_FIELDS):
            index = start_index + offset
            if index >= span_count:
                break
            text = (await tags_locator.nth(index).inner_text()).strip()
            if text:
                header_info[field_name] = text

        return header_info, has_former_name

    async def _get_page_school_badge_tags(self, page, timeout=5000):
        """
        从 .university-tags-pc 遍历 span，按文本匹配 985/211/双一流。
        返回 set，仅包含页面上实际出现的标签名。
        """
        badge_locator = page.locator(".university-tags-pc span")
        try:
            await badge_locator.first.wait_for(state="visible", timeout=timeout)
        except PlaywrightTimeoutError:
            return set()

        span_count = await badge_locator.count()
        found_tags = set()

        for index in range(span_count):
            text = (await badge_locator.nth(index).inner_text()).strip()
            if text in BADGE_TAG_TEXTS:
                found_tags.add(text)

        return found_tags

    async def _scrape_school(self, page, row_index, school_name, worker_id):
        url = self._build_url(school_name)
        result_text = "失败"

        try:
            await asyncio.sleep(random.uniform(*NAVIGATE_DELAY_SEC))
            await page.goto(url, wait_until="domcontentloaded")

            page_school_name = await self._get_page_school_name(page)
            if page_school_name is None:
                print(
                    f"[W{worker_id}] 【失败】未等到院校名元素: {school_name}"
                )
                self._save_status(row_index, "失败-未加载")
                result_text = "失败-未加载"
                return

            if page_school_name == "undefined":
                print(
                    f"[W{worker_id}] 【无效院校】夸克无此学校，"
                    f"查询「{school_name}」"
                )
                self._save_status(row_index, INVALID_SCHOOL_STATUS)
                result_text = INVALID_SCHOOL_STATUS
                return

            header_info, has_former_name = await self._get_page_school_header_info(page)
            badge_tags = await self._get_page_school_badge_tags(page)
            if header_info:
                print(
                    f"[W{worker_id}] 【院校标签】"
                    f"层次={header_info.get('层次', '')}，"
                    f"类型={header_info.get('类型', '')}，"
                    f"性质={header_info.get('性质', '')} "
                    f"[{school_name}]"
                )
            else:
                print(
                    f"[W{worker_id}] 【警告】未抓到院校标签，沿用源表原值: {school_name}"
                )

            if badge_tags:
                print(
                    f"[W{worker_id}] 【徽标标签】"
                    f"{', '.join(sorted(badge_tags, key=BADGE_TAG_FIELDS.index))} "
                    f"[{school_name}]"
                )
            else:
                print(
                    f"[W{worker_id}] 【提示】未抓到 985/211/双一流 徽标，"
                    f"沿用源表原值: {school_name}"
                )

            if has_former_name and page_school_name:
                print(
                    f"[W{worker_id}] 【曾用名】查询「{school_name}」，"
                    f"学校名称已更新为「{page_school_name}」"
                )
            elif not page_school_name:
                print(f"[W{worker_id}] 【警告】未读取到页面院校名称: {school_name}")
            elif page_school_name != school_name:
                print(
                    f"[W{worker_id}] 【提示】名称不一致，查询「{school_name}」，"
                    f"页面「{page_school_name}」（未检测到曾用名，保持源表名称）"
                )

            result_row = self._build_result_row(
                row_index,
                header_info,
                badge_info=badge_tags,
                page_school_name=page_school_name,
                has_former_name=has_former_name,
            )
            self._commit_scrape_success(row_index, result_row)
            result_text = "成功"

            header_scraped = sum(
                1 for field_name in HEADER_INFO_FIELDS if header_info.get(field_name)
            )
            badge_scraped = len(badge_tags)
            print(
                f"[W{worker_id}] 【成功】{school_name} "
                f"已写入结果表（层次标签 {header_scraped}/3，"
                f"徽标 {badge_scraped}/3）"
            )

        except Exception as err:
            self._save_status(row_index, "失败")
            result_text = f"异常: {err}"
            print(
                f"[W{worker_id}] 异常 | 行号{row_index} | "
                f"{school_name} | {err}"
            )
        finally:
            self._mark_task_finished(school_name, worker_id, result_text)

    async def _init_worker_pages(self, context):
        worker_pages = []
        for worker_id in range(self.concurrent_workers):
            page = await context.new_page()
            await page.evaluate(
                f"document.title = '[Worker {worker_id + 1}] 夸克高考-基本信息'"
            )
            worker_pages.append(page)
            print(f"  - Worker {worker_id + 1} 标签页已就绪")

        print(
            f"【浏览器】当前同一窗口内共 {len(context.pages)} 个标签页 "
            f"（请查看浏览器顶部标签栏）"
        )
        return worker_pages

    async def _worker(self, worker_id, page, queue):
        while True:
            try:
                row_index, school_name = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            self._mark_task_started(school_name, worker_id)
            self._save_status(row_index, "爬取中")
            await self._scrape_school(page, row_index, school_name, worker_id)
            await asyncio.sleep(random.uniform(*WORKER_TASK_DELAY_SEC))

    async def _run_async(self):
        start_time = time.time()

        self._init_source_excel()
        self._init_result_file()

        pending_tasks = []
        skip_count = 0
        success_count = 0
        retry_count = 0
        self._total_schools = len(self.df)

        for index, row in self.df.iterrows():
            status = str(row.get("状态", "")).strip()
            if status in SKIP_STATUSES:
                skip_count += 1
                success_count += 1
                continue

            school_name = str(row["学校名称"]).strip()
            if not school_name or school_name.lower() == "nan":
                continue

            if status:
                retry_count += 1

            pending_tasks.append((index, school_name))

        self._total_pending = len(pending_tasks)
        self._skip_count = skip_count
        self._started_count = 0
        self._finished_count = 0

        if not pending_tasks:
            print(
                f"没有待爬取的院校。"
                f"总计 {self._total_schools} 所，已成功 {success_count}，"
                f"跳过 {skip_count} 所。"
            )
            return

        retry_text = f"，含重试 {retry_count} 所" if retry_count else ""

        mode_text = (
            f"并发 {self.concurrent_workers} 标签页"
            if self.enable_concurrent
            else "单标签页顺序执行"
        )
        print(
            f"\n>>>> 结果表 {os.path.basename(self.result_path)}"
        )
        print(
            f">>>> 院校总计 {self._total_schools} 所 | "
            f"已成功 {success_count} | "
            f"跳过 {skip_count} | 本次待爬 {self._total_pending} 所{retry_text}"
        )
        print(f">>>> 模式：{mode_text}")
        print(
            f">>>> 状态表落盘：内存更新，每 {STATUS_FLUSH_INTERVAL_SEC:g}s 批量写入一次"
        )

        stop_status_flush = asyncio.Event()
        status_flush_task = asyncio.create_task(
            self._status_flush_loop(stop_status_flush)
        )

        try:
            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(headless=False)
                context = await browser.new_context(
                    viewport={"width": 1920, "height": 1080}
                )

                print(f"\n正在打开 {self.concurrent_workers} 个并发标签页...")
                worker_pages = await self._init_worker_pages(context)

                queue = asyncio.Queue()
                for task in pending_tasks:
                    await queue.put(task)

                workers = [
                    asyncio.create_task(
                        self._worker(
                            worker_id + 1, worker_pages[worker_id], queue
                        )
                    )
                    for worker_id in range(self.concurrent_workers)
                ]
                await asyncio.gather(*workers)

                for page in worker_pages:
                    if not page.is_closed():
                        await page.close()
                await context.close()
                await browser.close()
        finally:
            stop_status_flush.set()
            await status_flush_task

        end_time = time.time()
        print(
            f"\n>>>> 脚本全部执行完成！"
            f"本次完成 {self._finished_count}/{self._total_pending} 所，"
            f"总用时 {end_time - start_time:.2f} 秒"
        )

    def run(self):
        asyncio.run(self._run_async())


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))

    school_source_path = os.path.join(current_dir, "普通高校.csv")
    status_save_path = os.path.join(current_dir, "普通高校.csv")
    result_path = os.path.join(current_dir, "普通高校-基本信息.csv")

    scraper = QuarkBasicInfoScraper(
        school_source_path=school_source_path,
        status_save_path=status_save_path,
        result_path=result_path,
        enable_concurrent=ENABLE_CONCURRENT,
        concurrent_workers=CONCURRENT_WORKERS,
    )
    scraper.run()
