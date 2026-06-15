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

# 爬取目标（修改此处即可切换省份/年份）
TARGET_PROVINCE = "安徽"
TARGET_YEAR = "2025"
TARGET_BATCH = "本科批"
TARGET_GENRE = "首选物理"

# 并发开关：True 时启用多标签页并发；False 时单标签页顺序执行
ENABLE_CONCURRENT = True
CONCURRENT_WORKERS = 8

# 状态表落盘：内存更新后由后台定时批量写入，降低 Windows 下 os.replace 冲突
STATUS_FLUSH_INTERVAL_SEC = 1.0
FILE_WRITE_MAX_RETRIES = 5
FILE_WRITE_RETRY_DELAY_SEC = 0.3

# 结果表：源表带入字段
SOURCE_INFO_RESULT_COLUMNS = [
    "主管部门",
]

# 结果表：新源表额外字段（普通高校.csv 遍历时带入，不含序号/学校名称/状态）
# 源表「省份」写入「院校省份」，避免与爬取筛选条件「省份」重名
SOURCE_EXTRA_RESULT_COLUMNS = [
    "院校省份",
    "城市",
    "985",
    "211",
    "双一流",
    "类型",
    "层次",
    "性质",
]

SOURCE_EXTRA_COLUMN_MAP = {
    "院校省份": "省份",
    "城市": "城市",
    "985": "985",
    "211": "211",
    "双一流": "双一流",
    "类型": "类型",
    "层次": "层次",
    "性质": "性质",
}

CSV_READ_ENCODINGS = ("utf-8-sig", "utf-8", "gbk", "gb18030")

RESULT_COLUMNS = [
    "查询院校名称",
    "页面院校名称",
    *SOURCE_INFO_RESULT_COLUMNS,
    *SOURCE_EXTRA_RESULT_COLUMNS,
    "省份",
    "批次",
    "科类",
    "选科要求",
    "专业",
    "最低分",
    "最低位次",
    "人数",
    "批次线差",
    "备注",
]

MAJOR_NAME_INDEX = RESULT_COLUMNS.index("专业")


def read_csv_with_encodings(file_path):
    last_err = None
    for encoding in CSV_READ_ENCODINGS:
        try:
            return pd.read_csv(file_path, encoding=encoding)
        except UnicodeDecodeError as err:
            last_err = err
    raise last_err


def merge_major_with_remark(major_name, remark):
    """有备注时返回「专业-备注」，无备注时仅返回专业名。"""
    major_text = str(major_name or "").strip()
    remark_text = str(remark or "").strip()
    if remark_text:
        return f"{major_text}-{remark_text}"
    return major_text


VALUE_ONLY_FILTERS = ["批次", "科类"]
SKIP_STATUSES = {"成功", "无效数据", "本省未招生", "失败-无数据"}

MAJOR_CARD_SELECTOR = ".card-padding-zhuanye:has(.qk-title-text:has-text('专业分数线'))"
MAJOR_LIST_SELECTOR = ".content-List-li"
NO_ENROLLMENT_HINT_SELECTOR = ".nodata-fenshuxian"
NO_ENROLLMENT_SETTLE_SEC = 0.1
NO_ENROLLMENT_MIN_SIZE_PX = 20
LIST_CONTENT_READY_TIMEOUT_MS = 10000
SCORE_LOADING_SELECTOR = ".qk-loading.qk-loading-container"
SCORE_LOADING_APPEAR_TIMEOUT_MS = 3000
SCORE_LOADING_DISAPPEAR_TIMEOUT_MS = 8000
FILTER_VALUE_POLL_COUNT = 15
FILTER_VALUE_POLL_INTERVAL = 0.2

FILTER_TAB_SELECTORS = {
    "省份": ".select-tabs-tab-chengshi",
    "年份": ".select-tabs-tab-nianfen",
    "批次": ".select-tabs-tab-pici",
    "科类": ".select-tabs-tab-kemu",
}

FILTER_HAS_VALUE_LOCATORS = {
    "批次": ".qk-button-title",
    "科类": ".select-tabs-genre",
}


def build_quark_result_path(output_dir, province, year):
    return os.path.join(output_dir, f"夸克-{province}-{year}-院校专业表.csv")


class QuarkMajorsScraper:
    def __init__(
        self,
        school_source_path,
        status_save_path,
        result_path=None,
        target_province=TARGET_PROVINCE,
        target_year=TARGET_YEAR,
        target_batch=TARGET_BATCH,
        target_genre=TARGET_GENRE,
        enable_concurrent=ENABLE_CONCURRENT,
        concurrent_workers=CONCURRENT_WORKERS,
    ):
        self.school_source_path = school_source_path
        self.status_save_path = status_save_path
        self.target_province = target_province
        self.target_year = target_year
        self.target_batch = target_batch
        self.target_genre = target_genre
        self.required_filter_values = {
            "省份": target_province,
            "年份": target_year,
        }
        if result_path is None:
            output_dir = os.path.dirname(os.path.abspath(school_source_path))
            result_path = build_quark_result_path(
                output_dir, target_province, target_year
            )
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
            self.df.to_csv(temp_path, index=False, encoding="utf-8-sig")

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
        status_df.to_csv(temp_path, index=False, encoding="utf-8-sig")
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

    def _get_school_info(self, row_index):
        row = self.df.loc[row_index]
        school_info = {
            "主管部门": self._normalize_cell_value(row.get("主管部门", "")),
        }
        for result_col, source_col in SOURCE_EXTRA_COLUMN_MAP.items():
            school_info[result_col] = self._normalize_cell_value(
                row.get(source_col, "")
            )
        return school_info

    def _create_empty_result_file(self):
        with open(
            self.result_path, mode="w", encoding="utf-8-sig", newline=""
        ) as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(RESULT_COLUMNS)

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

    def _migrate_result_dataframe(self, existing_df):
        if existing_df is None or existing_df.empty:
            return pd.DataFrame(columns=RESULT_COLUMNS)

        source_columns = list(existing_df.columns)
        if source_columns == RESULT_COLUMNS:
            return existing_df[RESULT_COLUMNS].copy()

        migrated_df = pd.DataFrame(index=existing_df.index)
        for column_name in RESULT_COLUMNS:
            if column_name in existing_df.columns:
                migrated_df[column_name] = existing_df[column_name]
            else:
                migrated_df[column_name] = ""

        if "专业备注" in source_columns:
            migrated_df["备注"] = existing_df["专业备注"]

        return migrated_df[RESULT_COLUMNS]

    def _save_result_dataframe(self, result_df):
        def write_result(temp_path):
            result_df.to_csv(temp_path, index=False, encoding="utf-8-sig")

        self._atomic_replace_file(
            self.result_path, write_result, label="结果表"
        )

    def _migrate_legacy_xlsx_result(self):
        legacy_xlsx = f"{os.path.splitext(self.result_path)[0]}.xlsx"
        if not os.path.exists(legacy_xlsx):
            return False

        try:
            existing_df = pd.read_excel(legacy_xlsx, engine="openpyxl")
        except Exception as err:
            backup_path = f"{legacy_xlsx}.corrupt.bak"
            if os.path.exists(backup_path):
                os.remove(backup_path)
            os.replace(legacy_xlsx, backup_path)
            print(
                f"【警告】旧 xlsx 结果表已损坏，已备份为 {os.path.basename(backup_path)}"
                f"（原因: {err}）"
            )
            return False

        migrated_df = self._migrate_result_dataframe(existing_df)
        self._save_result_dataframe(migrated_df)
        backup_path = f"{legacy_xlsx}.bak"
        if os.path.exists(backup_path):
            os.remove(backup_path)
        os.replace(legacy_xlsx, backup_path)
        print(
            f"【提示】已将旧 xlsx 结果表迁移为 csv，保留 {len(migrated_df)} 行，"
            f"旧文件备份为 {os.path.basename(backup_path)}"
        )
        return True

    def _init_result_file(self):
        if not os.path.exists(self.result_path):
            if self._migrate_legacy_xlsx_result():
                return
            self._create_empty_result_file()
            print("【提示】结果表不存在，已创建空 csv，后续仅追加写入")
            return

        existing_df = self._read_result_dataframe(self.result_path)
        row_count = len(existing_df)

        if list(existing_df.columns) == RESULT_COLUMNS:
            print(f"【提示】结果表已存在 {row_count} 行，后续仅追加写入")
            return

        migrated_df = self._migrate_result_dataframe(existing_df)
        self._save_result_dataframe(migrated_df)
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
        jihuaparams = quote(
            f'{{"province":"{self.target_province}","year":"{self.target_year}",'
            f'"batch":"{self.target_batch}","genre":"{self.target_genre}"}}'
        )
        return (
            "https://vt.quark.cn/blm/gaokao-college-794/tab"
            f"?app=fen_shu_xian&university_name={encoded_school}&jihuaparams={jihuaparams}"
        )

    def _save_status(self, row_index, status):
        with self._save_lock:
            self.df.at[row_index, "状态"] = status
            self._status_dirty = True

    def _append_result_rows(self, rows_data):
        if not rows_data:
            return

        with self._save_lock:
            last_err = None
            for attempt in range(FILE_WRITE_MAX_RETRIES):
                try:
                    with open(
                        self.result_path,
                        mode="a",
                        encoding="utf-8-sig",
                        newline="",
                    ) as f:
                        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
                        writer.writerows(rows_data)
                        f.flush()
                        os.fsync(f.fileno())
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

    async def _get_locator_text(self, locator):
        if await locator.count() == 0:
            return ""
        return (await locator.first.inner_text()).strip()

    def _parse_subject_requirement(self, text):
        if not text:
            return ""
        if "选科要求：" in text:
            return text.split("选科要求：")[-1].strip()
        if "选科要求" in text:
            return (
                text.split("选科要求")[-1]
                .replace(":", "")
                .replace("：", "")
                .strip()
            )
        return text.strip()

    async def _get_page_school_name(self, page, timeout=5000):
        name_locator = page.locator(".university-logo-left .qk-title-text em")
        try:
            await name_locator.first.wait_for(state="visible", timeout=timeout)
        except PlaywrightTimeoutError:
            return None
        return (await name_locator.first.inner_text()).strip()

    async def _get_card_filters(self, card):
        genre_text = await self._get_locator_text(
            card.locator(f"{FILTER_TAB_SELECTORS['科类']} .select-tabs-genre span")
        )
        if not genre_text:
            genre_text = await self._get_locator_text(
                card.locator(f"{FILTER_TAB_SELECTORS['科类']} .select-tabs-genre")
            )

        return {
            "省份": await self._get_locator_text(
                card.locator(f"{FILTER_TAB_SELECTORS['省份']} .qk-button-title")
            ),
            "年份": await self._get_locator_text(
                card.locator(f"{FILTER_TAB_SELECTORS['年份']} .qk-button-title")
            ),
            "批次": await self._get_locator_text(
                card.locator(f"{FILTER_TAB_SELECTORS['批次']} .qk-button-title")
            ),
            "科类": genre_text,
        }

    async def _get_row_subject_requirement(self, row):
        sub_req_text = await self._get_locator_text(
            row.locator(".pc-subtitle-two-margin .qk-paragraph-text")
        )
        return self._parse_subject_requirement(sub_req_text)

    def _log_card_filters(self, filters, stage, school_name):
        def show(value):
            text = str(value).strip() if value is not None else ""
            return text if text else "(空)"

        print(
            f"【筛选】{stage} | "
            f"省份={show(filters.get('省份'))}，"
            f"年份={show(filters.get('年份'))}，"
            f"批次={show(filters.get('批次'))}，"
            f"科类={show(filters.get('科类'))} "
            f"[{school_name}]"
        )

    def _is_invalid_filter_value(self, actual, expected):
        actual_text = str(actual).strip()
        return bool(actual_text) and actual_text != expected

    def _validate_card_filters(self, filters):
        for key, expected in self.required_filter_values.items():
            actual = str(filters.get(key, "")).strip()
            if actual != expected:
                return False, key, expected, actual

        for key in VALUE_ONLY_FILTERS:
            actual = str(filters.get(key, "")).strip()
            if not actual:
                return False, key, "有值", actual

        return True, "", "", ""

    async def _has_major_list_data(self, card):
        return await card.locator(MAJOR_LIST_SELECTOR).count() > 0

    async def _is_nodata_visually_active(self, card):
        """nodata 默认 block 但高度为 0；宽高均 > 20px 才表示真正展示出来。"""
        no_data = card.locator(NO_ENROLLMENT_HINT_SELECTOR)
        if await no_data.count() == 0:
            return False

        return await no_data.first.evaluate(
            f"""(el) => {{
                const rect = el.getBoundingClientRect();
                return rect.width > {NO_ENROLLMENT_MIN_SIZE_PX}
                    && rect.height > {NO_ENROLLMENT_MIN_SIZE_PX};
            }}"""
        )

    async def _is_score_loading_visible(self, card):
        loading = card.locator(SCORE_LOADING_SELECTOR)
        if await loading.count() == 0:
            return False
        return await loading.first.is_visible()

    async def _wait_for_score_loading_hidden(self, card, timeout_ms):
        loading = card.locator(SCORE_LOADING_SELECTOR)
        if await loading.count() == 0:
            return True
        if not await loading.first.is_visible():
            return True

        try:
            await loading.first.wait_for(state="hidden", timeout=timeout_ms)
            return True
        except PlaywrightTimeoutError:
            return False

    async def _wait_loading_gone_then_settle(self, card, timeout_ms):
        """loading 可见则等到不可见，消失后再等 100ms。"""
        if not await self._is_score_loading_visible(card):
            return True

        if not await self._wait_for_score_loading_hidden(card, timeout_ms):
            return False

        await asyncio.sleep(NO_ENROLLMENT_SETTLE_SEC)
        return True

    async def _should_early_stop_no_enrollment(self, card):
        """列表无数据、loading 已消失并 settle、nodata 已展示 → 本省未招生。"""
        if await self._has_major_list_data(card):
            return False

        if not await self._wait_loading_gone_then_settle(
            card, SCORE_LOADING_DISAPPEAR_TIMEOUT_MS
        ):
            return False

        if await self._has_major_list_data(card):
            return False

        return await self._is_nodata_visually_active(card)

    async def _poll_list_or_no_enrollment(self, card, timeout_ms):
        """
        轮询列表与 nodata：有列表=有招生；
        loading 可见则 wait_for(hidden) 后再等 100ms；
        nodata 宽高>20px 则提前终止。
        返回 has_data | no_enrollment | timeout
        """
        deadline = time.monotonic() + timeout_ms / 1000

        while time.monotonic() < deadline:
            if await self._has_major_list_data(card):
                return "has_data"

            remaining_ms = max(1, int((deadline - time.monotonic()) * 1000))

            if await self._is_score_loading_visible(card):
                if not await self._wait_loading_gone_then_settle(card, remaining_ms):
                    break

                if await self._has_major_list_data(card):
                    return "has_data"

                if await self._is_nodata_visually_active(card):
                    return "no_enrollment"

                continue

            if await self._is_nodata_visually_active(card):
                return "no_enrollment"

            await asyncio.sleep(NO_ENROLLMENT_SETTLE_SEC)

        if await self._has_major_list_data(card):
            return "has_data"

        if await self._should_early_stop_no_enrollment(card):
            return "no_enrollment"

        return "timeout"

    async def _wait_for_score_module_settled(self, card, school_name):
        loading = card.locator(SCORE_LOADING_SELECTOR)
        loading_appeared = False

        appear_deadline = time.monotonic() + SCORE_LOADING_APPEAR_TIMEOUT_MS / 1000
        while time.monotonic() < appear_deadline:
            if await self._is_score_loading_visible(card):
                loading_appeared = True
                break
            await asyncio.sleep(FILTER_VALUE_POLL_INTERVAL)

        if loading_appeared:
            try:
                await loading.first.wait_for(
                    state="hidden", timeout=SCORE_LOADING_DISAPPEAR_TIMEOUT_MS
                )
            except PlaywrightTimeoutError:
                return False, "load", "分数模块loading未消失"
        else:
            await asyncio.sleep(0.5)

        poll_result = await self._poll_list_or_no_enrollment(
            card, LIST_CONTENT_READY_TIMEOUT_MS
        )
        if poll_result == "no_enrollment":
            filters = await self._get_card_filters(card)
            self._log_card_filters(filters, "分数模块加载后nodata显示", school_name)
            return False, "no_enrollment", "nodata-fenshuxian已展示"

        return True, "", ""

    async def _wait_for_filter_has_value(self, card, field_name, school_name):
        tab_selector = FILTER_TAB_SELECTORS[field_name]
        value_selector = FILTER_HAS_VALUE_LOCATORS.get(
            field_name, ".qk-button-title"
        )
        value_locator = card.locator(f"{tab_selector} {value_selector}").first

        try:
            await value_locator.wait_for(state="visible", timeout=5000)
        except PlaywrightTimeoutError:
            filters = await self._get_card_filters(card)
            self._log_card_filters(filters, f"{field_name}筛选框未出现", school_name)
            return False, "load", f"{field_name}筛选框未出现", filters

        for _ in range(FILTER_VALUE_POLL_COUNT):
            if await self._should_early_stop_no_enrollment(card):
                filters = await self._get_card_filters(card)
                self._log_card_filters(
                    filters, f"{field_name}等待期间本省未招生", school_name
                )
                return False, "no_enrollment", "nodata-fenshuxian已展示", filters

            filters = await self._get_card_filters(card)
            actual = str(filters.get(field_name, "")).strip()
            if actual:
                self._log_card_filters(filters, f"{field_name}有值", school_name)
                return True, "", "", filters
            await asyncio.sleep(FILTER_VALUE_POLL_INTERVAL)

        if await self._should_early_stop_no_enrollment(card):
            filters = await self._get_card_filters(card)
            self._log_card_filters(
                filters, f"{field_name}超时后本省未招生", school_name
            )
            return False, "no_enrollment", "nodata-fenshuxian已展示", filters

        filters = await self._get_card_filters(card)
        self._log_card_filters(filters, f"{field_name}值未就绪", school_name)
        return False, "load", f"{field_name}值为空", filters

    async def _wait_for_filter_value(self, card, field_name, expected_value, school_name):
        tab_selector = FILTER_TAB_SELECTORS[field_name]
        title_locator = card.locator(f"{tab_selector} .qk-button-title").first

        try:
            await title_locator.wait_for(state="visible", timeout=5000)
        except PlaywrightTimeoutError:
            filters = await self._get_card_filters(card)
            self._log_card_filters(filters, f"{field_name}筛选框未出现", school_name)
            return False, "load", f"{field_name}筛选框未出现", filters

        for _ in range(15):
            actual = await self._get_locator_text(
                card.locator(f"{tab_selector} .qk-button-title")
            )
            if actual == expected_value:
                filters = await self._get_card_filters(card)
                self._log_card_filters(filters, f"{field_name}就绪", school_name)
                return True, "", "", filters
            if self._is_invalid_filter_value(actual, expected_value):
                filters = await self._get_card_filters(card)
                self._log_card_filters(filters, f"{field_name}无效", school_name)
                return False, "invalid", f"{field_name}={actual}", filters
            await asyncio.sleep(0.2)

        filters = await self._get_card_filters(card)
        actual = str(filters.get(field_name, "")).strip()
        if self._is_invalid_filter_value(actual, expected_value):
            self._log_card_filters(filters, f"{field_name}无效", school_name)
            return False, "invalid", f"{field_name}={actual}", filters

        self._log_card_filters(filters, f"{field_name}值未就绪", school_name)
        return False, "load", f"{field_name}值未就绪", filters

    async def _wait_for_card_filters(self, card, school_name):
        province_ready, province_error_type, province_error, filters = (
            await self._wait_for_filter_value(
                card, "省份", self.required_filter_values["省份"], school_name
            )
        )
        if not province_ready:
            return False, province_error_type, province_error, filters

        year_ready, year_error_type, year_error, filters = (
            await self._wait_for_filter_value(
                card, "年份", self.required_filter_values["年份"], school_name
            )
        )
        if not year_ready:
            return False, year_error_type, year_error, filters

        module_ready, module_error_type, module_error = (
            await self._wait_for_score_module_settled(card, school_name)
        )
        if not module_ready:
            filters = await self._get_card_filters(card)
            return False, module_error_type, module_error, filters

        for field_name in VALUE_ONLY_FILTERS:
            value_ready, value_error_type, value_error, filters = (
                await self._wait_for_filter_has_value(card, field_name, school_name)
            )
            if not value_ready:
                return False, value_error_type, value_error, filters

        filters = await self._get_card_filters(card)
        self._log_card_filters(filters, "筛选就绪", school_name)
        return True, "", "", filters

    def _get_major_card(self, page):
        return page.locator(MAJOR_CARD_SELECTOR).first

    async def _has_no_local_enrollment_hint(self, card):
        return await self._should_early_stop_no_enrollment(card)

    async def _parse_major_row(
        self, row, filters, school_name, page_school_name, school_info
    ):
        major_name = await self._get_locator_text(
            row.locator(".content-List-major .qk-paragraph-text")
        )
        low_score = await self._get_locator_text(
            row.locator(".content-List-low_score .qk-paragraph-text")
        )
        low_rank = await self._get_locator_text(
            row.locator(".content-List-low_rank .qk-paragraph-text")
        )
        enroll_count = await self._get_locator_text(
            row.locator(".content-List-luqurenshu .qk-paragraph-text")
        )
        score_diff = await self._get_locator_text(
            row.locator(".content-List-low_score_diff .qk-paragraph-text")
        )
        remark = await self._get_locator_text(
            row.locator(".pc-subtitle-margin .qk-paragraph-text")
        )
        subject_requirement = await self._get_row_subject_requirement(row)

        if not major_name:
            return None

        major_display = merge_major_with_remark(major_name, remark)

        return [
            school_name,
            page_school_name,
            school_info["主管部门"],
            school_info["院校省份"],
            school_info["城市"],
            school_info["985"],
            school_info["211"],
            school_info["双一流"],
            school_info["类型"],
            school_info["层次"],
            school_info["性质"],
            filters["省份"],
            filters["批次"],
            filters["科类"],
            subject_requirement,
            major_display,
            low_score,
            low_rank,
            enroll_count,
            score_diff,
            remark,
        ]

    async def _scrape_school(self, page, row_index, school_name, worker_id):
        url = self._build_url(school_name)
        result_text = "失败"

        try:
            await page.bring_to_front()
            await asyncio.sleep(random.uniform(0.1, 0.32))
            await page.goto(url, wait_until="domcontentloaded")

            page_school_name = await self._get_page_school_name(page)
            if page_school_name is None:
                print(
                    f"[W{worker_id}] 【失败】未等到院校名 em 元素: {school_name}"
                )
                await asyncio.sleep(3)
                self._save_status(row_index, "失败-未加载")
                result_text = "失败-未加载"
                return

            if page_school_name == "undefined":
                print(
                    f"[W{worker_id}] 【本省未招生】页面院校名为 undefined 字符串，"
                    f"判定该省未招生: {school_name}"
                )
                self._save_status(row_index, "本省未招生")
                result_text = "本省未招生"
                return

            card = self._get_major_card(page)
            try:
                await card.wait_for(state="visible", timeout=10000)
            except PlaywrightTimeoutError:
                print(
                    f"[W{worker_id}] 【失败】未找到专业分数线模块: {school_name}"
                )
                await asyncio.sleep(3)
                self._save_status(row_index, "失败-未加载")
                result_text = "失败-未加载"
                return

            if not page_school_name:
                print(f"[W{worker_id}] 【警告】未读取到页面院校名称: {school_name}")
            elif page_school_name != school_name:
                print(
                    f"[W{worker_id}] 【提示】名称不一致，查询「{school_name}」，"
                    f"页面「{page_school_name}」"
                )

            filters_ready, error_type, filter_error, filters = (
                await self._wait_for_card_filters(card, school_name)
            )
            if not filters_ready:
                if error_type == "invalid":
                    print(
                        f"[W{worker_id}] 【无效数据】{filter_error}，"
                        f"非{self.target_province}/{self.target_year}: {school_name}"
                    )
                    self._save_status(row_index, "无效数据")
                    result_text = "无效数据"
                elif error_type == "no_enrollment":
                    print(
                        f"[W{worker_id}] 【本省未招生】"
                        f"{self.target_province}/{self.target_year} 已就绪，"
                        f"{filter_error or 'nodata-fenshuxian已展示'}: "
                        f"{school_name}"
                    )
                    self._save_status(row_index, "本省未招生")
                    result_text = "本省未招生"
                else:
                    print(f"[W{worker_id}] 【失败】{filter_error}: {school_name}")
                    self._save_status(row_index, "失败-未加载")
                    result_text = "失败-未加载"
                return

            filter_ok, filter_key, expected, actual = self._validate_card_filters(
                filters
            )
            if not filter_ok:
                self._log_card_filters(filters, "校验失败", school_name)
                actual_text = str(actual).strip()
                if filter_key in self.required_filter_values and actual_text:
                    print(
                        f"[W{worker_id}] 【无效数据】「{filter_key}」"
                        f"期望「{expected}」实际「{actual_text}」: {school_name}"
                    )
                    self._save_status(row_index, "无效数据")
                    result_text = "无效数据"
                else:
                    print(
                        f"[W{worker_id}] 【失败】「{filter_key}」不符: {school_name}"
                    )
                    self._save_status(row_index, f"失败-{filter_key}错误")
                    result_text = f"失败-{filter_key}错误"
                return

            print(
                f"[W{worker_id}] 【成功】{filters['省份']} {filters['年份']} "
                f"{filters['批次']} {filters['科类']}，"
                f"页面院校: {page_school_name or '未知'}，开始提取: {school_name}"
            )

            list_poll = await self._poll_list_or_no_enrollment(
                card, LIST_CONTENT_READY_TIMEOUT_MS
            )
            if list_poll == "no_enrollment":
                print(
                    f"[W{worker_id}] 【本省未招生】"
                    f"{self.target_province}/{self.target_year} 下无专业列表，"
                    f"且 nodata 元素已展示: {school_name}"
                )
                self._save_status(row_index, "本省未招生")
                result_text = "本省未招生"
                return

            major_rows = card.locator(MAJOR_LIST_SELECTOR)
            row_count = await major_rows.count()
            has_no_enrollment_hint = await self._has_no_local_enrollment_hint(card)

            if row_count == 0 and has_no_enrollment_hint:
                print(
                    f"[W{worker_id}] 【本省未招生】"
                    f"{self.target_province}/{self.target_year} 下无专业列表，"
                    f"且 nodata 元素已展示: {school_name}"
                )
                self._save_status(row_index, "本省未招生")
                result_text = "本省未招生"
                return

            school_info = self._get_school_info(row_index)
            results = []
            result_rows = []

            for index in range(row_count):
                try:
                    parsed_row = await self._parse_major_row(
                        major_rows.nth(index),
                        filters,
                        school_name,
                        page_school_name,
                        school_info,
                    )
                    if not parsed_row:
                        continue
                    result_rows.append(parsed_row)
                    results.append(parsed_row[MAJOR_NAME_INDEX])
                except Exception as row_err:
                    print(f"[W{worker_id}] 解析单行出错，跳过: {row_err}")
                    continue

            self._append_result_rows(result_rows)

            if results:
                self._save_status(row_index, "成功")
                result_text = f"成功 {len(results)} 条"
                print(
                    f"[W{worker_id}] 【成功】{school_name} "
                    f"共 {len(results)} 条专业"
                )
            else:
                self._save_status(row_index, "失败-未解析到数据")
                result_text = "失败-未解析到数据"
                print(
                    f"[W{worker_id}] 【失败】未解析到数据: {school_name}"
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
            await page.evaluate(f"document.title = '[Worker {worker_id + 1}] 夸克高考'")
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
            await self._scrape_school(page, row_index, school_name, worker_id)
            await asyncio.sleep(random.uniform(1, 2))

    async def _run_async(self):
        start_time = time.time()

        self._init_source_excel()
        self._init_result_file()

        pending_tasks = []
        skip_count = 0
        success_count = 0
        invalid_count = 0
        no_enrollment_count = 0
        self._total_schools = len(self.df)

        for index, row in self.df.iterrows():
            status = str(row.get("状态", "")).strip()
            if status in SKIP_STATUSES:
                skip_count += 1
                if status == "成功":
                    success_count += 1
                elif status == "无效数据":
                    invalid_count += 1
                elif status in {"本省未招生", "失败-无数据"}:
                    no_enrollment_count += 1
                continue

            school_name = str(row["学校名称"]).strip()
            if not school_name or school_name.lower() == "nan":
                continue

            pending_tasks.append((index, school_name))

        self._total_pending = len(pending_tasks)
        self._skip_count = skip_count
        self._started_count = 0
        self._finished_count = 0

        if not pending_tasks:
            print(
                f"没有待爬取的院校。"
                f"总计 {self._total_schools} 所，已成功 {success_count}，"
                f"无效数据 {invalid_count}，本省未招生 {no_enrollment_count}，"
                f"跳过 {skip_count} 所。"
            )
            return

        mode_text = (
            f"并发 {self.concurrent_workers} 标签页"
            if self.enable_concurrent
            else "单标签页顺序执行"
        )
        print(
            f"\n>>>> 目标 {self.target_province}/{self.target_year} | "
            f"结果表 {os.path.basename(self.result_path)}"
        )
        print(
            f">>>> 院校总计 {self._total_schools} 所 | "
            f"已成功 {success_count} | 无效数据 {invalid_count} | "
            f"本省未招生 {no_enrollment_count} | "
            f"跳过 {skip_count} | 本次待爬 {self._total_pending} 所"
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

    scraper = QuarkMajorsScraper(
        school_source_path=school_source_path,
        status_save_path=status_save_path,
        target_province=TARGET_PROVINCE,
        target_year=TARGET_YEAR,
        target_batch=TARGET_BATCH,
        target_genre=TARGET_GENRE,
        enable_concurrent=ENABLE_CONCURRENT,
        concurrent_workers=CONCURRENT_WORKERS,
    )
    scraper.run()
