import os
import time
import asyncio
import random
import csv
import threading
from datetime import date

import pandas as pd
from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
)

from playwright_utils import get_profile_dir, USER_AGENT, LOGIN_URL

# 爬取目标（修改此处即可切换省份/年份/科类/批次）
TARGET_PROVINCE = "广东"
TARGET_YEAR = "2025"
TARGET_BATCH = "本科批"
TARGET_SUBJECT = "物理类"

# 并发开关：True 时启用多标签页并发；False 时单标签页顺序执行
ENABLE_CONCURRENT = True
CONCURRENT_WORKERS = 1

# 状态表落盘：内存更新后由后台定时批量写入，降低 Windows 下 os.replace 冲突
STATUS_FLUSH_INTERVAL_SEC = 1.0
FILE_WRITE_MAX_RETRIES = 5
FILE_WRITE_RETRY_DELAY_SEC = 0.3

CSV_READ_ENCODINGS = ("utf-8-sig", "utf-8", "gbk", "gb18030")

RESULT_COLUMNS = [
    "查询院校ID",
    "查询院校名称",
    "页面院校名称",
    "页面所在地",
    "性质",
    "类型",
    "主管部门",
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

VALUE_ONLY_FILTERS = ["批次", "科类"]
SKIP_STATUSES = {
    "成功",
    "无效",
    "无效数据",
    "本省未招生",
    "失败-无数据",
}

SCORELINE_SELECTOR = "#scoreline"
SCHOOL_INFO_SELECTOR = ".school-tab_info__3x6H6"
SCHOOL_NAME_SELECTOR = ".school-tab_name__3pOZK"
SCHOOL_ADDRESS_SELECTOR = ".school-tab_adress__1WWI_"
SCHOOL_CORE_TAGS_SELECTOR = ".school-tab_coreTags__31I0N span"
SCHOOL_PROFILE_TIMEOUT_MS = 8000
NATURE_KEYWORDS = frozenset({"公办", "民办"})
SCHOOL_TYPE_KEYWORDS = frozenset(
    {"综合", "理工", "师范", "财经", "农林", "医药", "艺术", "体育", "军事", "语言"}
)
TABLE_ROW_SELECTOR = "table.tb-normal tbody tr"
PAGINATION_BOX_SELECTOR = ".pagination_box"
PAGINATION_NEXT_SELECTOR = ".ant-pagination-next:not(.ant-pagination-disabled)"
PAGINATION_ITEM_SELECTOR = ".ant-pagination-item"
MAX_PAGINATION_PAGES = 200
FILTER_BAR_SELECTORS = (
    ".slt-drop.flex-fsx",
    ".slt-drop",
    ":scope",
)
FILTER_VALUE_SELECTOR = ".ant-select-selection-selected-value"
FILTER_FIELD_ORDER = ("省份", "年份", "科类", "批次")

SCORELINE_READY_TIMEOUT_MS = 20000
TABLE_READY_TIMEOUT_MS = 10000
FILTER_VALUE_POLL_COUNT = 20
FILTER_VALUE_POLL_INTERVAL = 0.2
PAGE_CHANGE_TIMEOUT_MS = 8000
PAGE_CHANGE_DEBOUNCE_SEC = 0.3
NO_DATA_SETTLE_SEC = 0.15
NO_ENROLLMENT_MIN_BOX_SIZE = 50
NO_ENROLLMENT_SELECTORS = (
    ".nodata_nodata__1Ey7Y.show",
    "[class*='nodata_nodata'].show",
    ".nodata_nodata__1Ey7Y",
)
NO_ENROLLMENT_TEXT_SELECTORS = (
    ".nodata_customText__3jJyM",
    "[class*='nodata_customText']",
)
DEFAULT_NO_ENROLLMENT_TEXT = "当前地区学校暂未招生或分数未公布"

BLOCKING_MODAL_WRAP_SELECTOR = ".ant-modal-wrap"
MODAL_DISMISS_TIMEOUT_MS = 3000
MODAL_DISMISS_POLL_INTERVAL = 0.15
BLOCKING_MODAL_RULES = (
    {
        "name": "会员推荐弹窗",
        "wrap_selector": ".ant-modal-wrap",
        "body_selector": ".activate-member_activateMember__3Aock",
        "close_selector": ".activate-member_close__DskC5, .icon_close",
    },
    {
        "name": "会员推荐弹窗(直连)",
        "wrap_selector": "",
        "body_selector": ".activate-member_activateMember__3Aock",
        "close_selector": ".activate-member_close__DskC5, .icon_close",
    },
    {
        "name": "VIP解锁弹窗",
        "wrap_selector": BLOCKING_MODAL_WRAP_SELECTOR,
        "body_selector": ".main-nav_openVipOpModalBox__1My1E",
        "close_selector": ".icon_close",
    },
)

ID_COLUMN_CANDIDATES = ("id", "ID", "代码")
SCHOOL_NAME_COLUMN_CANDIDATES = ("学校名称", "院校名称")


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


def build_gaokao_result_path(output_dir, province, year):
    return os.path.join(
        output_dir, f"掌上高考-{province}-{year}-院校专业表.csv"
    )


def resolve_id_column(columns):
    for column_name in ID_COLUMN_CANDIDATES:
        if column_name in columns:
            return column_name
    raise KeyError(
        f"状态表缺少院校 ID 列，需要以下之一: {', '.join(ID_COLUMN_CANDIDATES)}"
    )


def resolve_school_name_column(columns):
    for column_name in SCHOOL_NAME_COLUMN_CANDIDATES:
        if column_name in columns:
            return column_name
    raise KeyError(
        f"状态表缺少院校名称列，需要以下之一: {', '.join(SCHOOL_NAME_COLUMN_CANDIDATES)}"
    )


class GaokaoScorelineScraper:
    def __init__(
        self,
        school_source_path,
        status_save_path,
        result_path=None,
        target_province=TARGET_PROVINCE,
        target_year=TARGET_YEAR,
        target_batch=TARGET_BATCH,
        target_subject=TARGET_SUBJECT,
        enable_concurrent=ENABLE_CONCURRENT,
        concurrent_workers=CONCURRENT_WORKERS,
    ):
        self.school_source_path = school_source_path
        self.status_save_path = status_save_path
        self.target_province = target_province
        self.target_year = target_year
        self.target_batch = target_batch
        self.target_subject = target_subject
        self.required_filter_values = {
            "省份": target_province,
            "年份": target_year,
            "批次": target_batch,
            "科类": target_subject,
        }
        if result_path is None:
            output_dir = os.path.dirname(os.path.abspath(school_source_path))
            result_path = build_gaokao_result_path(
                output_dir, target_province, target_year
            )
        self.result_path = result_path
        self.enable_concurrent = enable_concurrent
        self.concurrent_workers = max(
            1, concurrent_workers if enable_concurrent else 1
        )

        self.df = None
        self.id_column = None
        self.school_name_column = None
        self._status_lock = threading.Lock()
        self._result_lock = threading.Lock()
        self._progress_lock = threading.Lock()
        self._status_dirty = False
        self._total_schools = 0
        self._total_pending = 0
        self._skip_count = 0
        self._started_count = 0
        self._finished_count = 0

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
        with self._status_lock:
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
                with self._status_lock:
                    self._status_dirty = True
                print(f"【警告】状态表落盘失败，将在下次定时重试: {err}")

            if stop_event.is_set():
                break
            await asyncio.sleep(STATUS_FLUSH_INTERVAL_SEC)

        try:
            self._flush_status_to_disk()
        except PermissionError as err:
            with self._status_lock:
                self._status_dirty = True
            print(f"【警告】退出前状态表落盘失败: {err}")

    def _load_status_dataframe(self):
        if not os.path.exists(self.status_save_path):
            return read_csv_with_encodings(self.school_source_path)

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
            return read_csv_with_encodings(self.school_source_path)

    def _init_source_table(self):
        self.df = self._load_status_dataframe()
        self.id_column = resolve_id_column(self.df.columns)
        self.school_name_column = resolve_school_name_column(self.df.columns)

        if "状态" not in self.df.columns:
            self.df["状态"] = ""
        self.df["状态"] = self.df["状态"].fillna("").astype(str)

        if "错误信息" not in self.df.columns:
            self.df["错误信息"] = ""
        self.df["错误信息"] = self.df["错误信息"].fillna("").astype(str)

        if "日期" not in self.df.columns:
            self.df["日期"] = ""
        self.df["日期"] = self.df["日期"].fillna("").astype(str)

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

    def _get_school_meta(self, row_index):
        row = self.df.loc[row_index]
        return {
            "school_id": self._normalize_cell_value(row.get(self.id_column, "")),
            "school_name": self._normalize_cell_value(
                row.get(self.school_name_column, "")
            ),
            "source_nature": self._normalize_cell_value(row.get("性质", "")),
            "source_type": self._normalize_cell_value(row.get("类型", "")),
            "source_department": self._normalize_cell_value(
                row.get("主管部门", "")
            ),
        }

    @staticmethod
    def _is_school_type_tag(tag):
        if tag.endswith("类"):
            return True
        return tag in SCHOOL_TYPE_KEYWORDS

    @classmethod
    def _parse_page_core_tags(cls, core_tags):
        nature = ""
        school_type = ""
        affiliations = []

        for raw_tag in core_tags:
            tag = str(raw_tag or "").strip()
            if not tag:
                continue
            if tag in NATURE_KEYWORDS and not nature:
                nature = tag
                continue
            if cls._is_school_type_tag(tag) and not school_type:
                school_type = tag
                continue
            affiliations.append(tag)

        return {
            "nature": nature,
            "school_type": school_type,
            "department": "、".join(affiliations),
        }

    @staticmethod
    def _merge_profile_field(page_value, source_value):
        page_text = str(page_value or "").strip()
        source_text = str(source_value or "").strip()
        return page_text or source_text

    def _merge_school_profile(self, page_profile, school_meta):
        parsed_core = page_profile.get("parsed_core", {})
        return {
            "page_school_name": page_profile.get("name", ""),
            "page_address": page_profile.get("address", ""),
            "nature": self._merge_profile_field(
                parsed_core.get("nature"), school_meta.get("source_nature")
            ),
            "school_type": self._merge_profile_field(
                parsed_core.get("school_type"), school_meta.get("source_type")
            ),
            "department": self._merge_profile_field(
                parsed_core.get("department"), school_meta.get("source_department")
            ),
        }

    def _create_empty_result_file(self):
        with open(
            self.result_path, mode="w", encoding="utf-8-sig", newline=""
        ) as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(RESULT_COLUMNS)

    def _init_result_file(self):
        if not os.path.exists(self.result_path):
            self._create_empty_result_file()
            print("【提示】结果表不存在，已创建空 csv，后续仅追加写入")
            return

        try:
            existing_df = read_csv_with_encodings(self.result_path)
        except Exception as err:
            backup_path = f"{self.result_path}.corrupt.bak"
            if os.path.exists(backup_path):
                os.remove(backup_path)
            os.replace(self.result_path, backup_path)
            print(
                f"【警告】结果文件已损坏，已备份为 {os.path.basename(backup_path)}，"
                f"将重建空表（原因: {err}）"
            )
            self._create_empty_result_file()
            return

        if list(existing_df.columns) == RESULT_COLUMNS:
            print(f"【提示】结果表已存在 {len(existing_df)} 行，后续仅追加写入")
            return

        migrated_df = pd.DataFrame(columns=RESULT_COLUMNS)
        for column_name in RESULT_COLUMNS:
            if column_name in existing_df.columns:
                migrated_df[column_name] = existing_df[column_name]
            else:
                migrated_df[column_name] = ""
        self._atomic_replace_file(
            self.result_path,
            lambda temp_path: migrated_df.to_csv(
                temp_path, index=False, encoding="utf-8-sig"
            ),
            label="结果表",
        )
        print(f"【提示】结果表表头已对齐，保留 {len(migrated_df)} 行")

    def _mark_task_started(self, school_label, worker_id):
        with self._progress_lock:
            self._started_count += 1
            traversed = self._skip_count + self._started_count
        print(
            f"[W{worker_id}] [{traversed}/{self._total_schools}] "
            f"开始 | {school_label}"
        )
        return traversed

    def _mark_task_finished(self, school_label, worker_id, result_text):
        with self._progress_lock:
            self._finished_count += 1
            traversed = self._skip_count + self._finished_count
            remaining = self._total_pending - self._finished_count
        print(
            f"[W{worker_id}] [{traversed}/{self._total_schools}] "
            f"完成(剩{remaining}) | {school_label} | {result_text}"
        )

    def _build_url(self, school_id):
        return f"https://www.gaokao.cn/school/{school_id}/provinceline"

    def _save_status(self, row_index, status, error_message=""):
        with self._status_lock:
            self.df.at[row_index, "状态"] = status
            if error_message:
                self.df.at[row_index, "错误信息"] = error_message
            self.df.at[row_index, "日期"] = date.today().strftime("%Y/%m/%d")
            self._status_dirty = True

    def _append_result_rows(self, rows_data):
        if not rows_data:
            return

        with self._result_lock:
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

    def _parse_score_rank(self, text):
        score_text = str(text or "").strip()
        if "/" in score_text:
            score_part, rank_part = score_text.split("/", 1)
            return score_part.strip(), rank_part.strip()
        return score_text, ""

    def _log_filters(self, filters, stage, school_label):
        def show(value):
            text = str(value).strip() if value is not None else ""
            return text if text else "(空)"

        print(
            f"【筛选】{stage} | "
            f"省份={show(filters.get('省份'))}，"
            f"年份={show(filters.get('年份'))}，"
            f"批次={show(filters.get('批次'))}，"
            f"科类={show(filters.get('科类'))} "
            f"[{school_label}]"
        )

    def _validate_filters(self, filters):
        for key, expected in self.required_filter_values.items():
            actual = str(filters.get(key, "")).strip()
            if actual != expected:
                return False, key, expected, actual
        return True, "", "", ""

    async def _try_dismiss_modal_rule(
        self, page, rule, school_label="", worker_id=0
    ):
        label_suffix = f" [{school_label}]" if school_label else ""
        body_selector = rule.get("body_selector", "")
        wrap_selector = rule.get("wrap_selector", "")
        close_selector = rule.get("close_selector", "")

        if body_selector and wrap_selector:
            modal_root = page.locator(wrap_selector).filter(
                has=page.locator(body_selector)
            ).first
        elif body_selector:
            modal_root = page.locator(body_selector).first
        elif wrap_selector:
            modal_root = page.locator(wrap_selector).first
        else:
            return False

        try:
            if await modal_root.count() == 0 or not await modal_root.is_visible():
                return False
        except Exception:
            return False

        close_btn = (
            modal_root.locator(close_selector).first
            if close_selector
            else modal_root
        )
        try:
            if await close_btn.count() == 0 or not await close_btn.is_visible():
                return False
            await close_btn.click(timeout=2000)
            try:
                await modal_root.wait_for(
                    state="hidden", timeout=MODAL_DISMISS_TIMEOUT_MS
                )
            except PlaywrightTimeoutError:
                if body_selector:
                    body_locator = page.locator(body_selector).first
                    if await body_locator.count() > 0:
                        await body_locator.wait_for(
                            state="hidden", timeout=MODAL_DISMISS_TIMEOUT_MS
                        )
            print(
                f"[W{worker_id}] 【弹窗】已关闭{rule['name']}{label_suffix}"
            )
            await asyncio.sleep(MODAL_DISMISS_POLL_INTERVAL)
            return True
        except Exception as err:
            print(
                f"[W{worker_id}] 【弹窗】关闭{rule['name']}失败"
                f"{label_suffix}: {err}"
            )
            return False

    async def _dismiss_blocking_modals(self, page, school_label="", worker_id=0):
        """关闭会员推荐、VIP 解锁等遮挡操作的弹窗，返回本轮是否关闭过弹窗。"""
        if page is None:
            return False

        dismissed_any = False

        for _ in range(3):
            dismissed_this_round = False

            for rule in BLOCKING_MODAL_RULES:
                if await self._try_dismiss_modal_rule(
                    page, rule, school_label, worker_id
                ):
                    dismissed_any = True
                    dismissed_this_round = True
                    break

            if not dismissed_this_round:
                break

        return dismissed_any

    async def _resolve_filter_bar(self, scoreline):
        for selector in FILTER_BAR_SELECTORS:
            filter_bar = scoreline.locator(selector).first
            try:
                if await filter_bar.count() == 0 or not await filter_bar.is_visible():
                    continue
                value_count = await filter_bar.locator(FILTER_VALUE_SELECTOR).count()
                if value_count >= len(FILTER_FIELD_ORDER):
                    return filter_bar
                if value_count > 0:
                    return filter_bar
            except Exception:
                continue

        return scoreline

    async def _read_filter_value_text(self, value_locator):
        title = await value_locator.get_attribute("title")
        if title and str(title).strip():
            return str(title).strip()
        return (await value_locator.inner_text()).strip()

    async def _get_scoreline_filters(self, scoreline):
        filter_bar = await self._resolve_filter_bar(scoreline)
        values = filter_bar.locator(FILTER_VALUE_SELECTOR)
        count = await values.count()
        texts = []
        for index in range(min(count, len(FILTER_FIELD_ORDER))):
            texts.append(await self._read_filter_value_text(values.nth(index)))

        return {
            field_name: texts[index] if index < len(texts) else ""
            for index, field_name in enumerate(FILTER_FIELD_ORDER)
        }

    async def _get_filter_value_locator(self, scoreline, field_name):
        field_index = FILTER_FIELD_ORDER.index(field_name)
        filter_bar = await self._resolve_filter_bar(scoreline)
        return filter_bar.locator(FILTER_VALUE_SELECTOR).nth(field_index)

    async def _get_table_fingerprint(self, scoreline):
        return await scoreline.locator("table.tb-normal tbody").first.evaluate(
            """(tbody) => (tbody.textContent || '').trim()"""
        )

    async def _wait_for_table_change(self, scoreline, previous_fingerprint, timeout_ms):
        deadline = time.monotonic() + timeout_ms / 1000
        while time.monotonic() < deadline:
            current_fingerprint = await self._get_table_fingerprint(scoreline)
            if current_fingerprint != previous_fingerprint:
                await asyncio.sleep(PAGE_CHANGE_DEBOUNCE_SEC)
                return True
            await asyncio.sleep(0.1)
        return False

    async def _has_table_rows(self, scoreline):
        return await scoreline.locator(TABLE_ROW_SELECTOR).count() > 0

    async def _detect_no_enrollment_state(self, scoreline):
        for selector in NO_ENROLLMENT_SELECTORS:
            nodata = scoreline.locator(selector).first
            try:
                if await nodata.count() == 0 or not await nodata.is_visible():
                    continue

                box = await nodata.bounding_box()
                if not box:
                    continue
                if (
                    box.get("width", 0) <= NO_ENROLLMENT_MIN_BOX_SIZE
                    or box.get("height", 0) <= NO_ENROLLMENT_MIN_BOX_SIZE
                ):
                    continue

                message = DEFAULT_NO_ENROLLMENT_TEXT
                for text_selector in NO_ENROLLMENT_TEXT_SELECTORS:
                    text_locator = nodata.locator(text_selector).first
                    if await text_locator.count() == 0:
                        continue
                    text = (await text_locator.inner_text()).strip()
                    if text:
                        message = text
                        break

                return True, message
            except Exception:
                continue

        return False, ""

    async def _wait_for_scoreline_content(self, scoreline):
        deadline = time.monotonic() + TABLE_READY_TIMEOUT_MS / 1000
        while time.monotonic() < deadline:
            if await self._has_table_rows(scoreline):
                return "has_rows", ""

            is_no_enrollment, message = await self._detect_no_enrollment_state(
                scoreline
            )
            if is_no_enrollment:
                return "no_enrollment", message

            await asyncio.sleep(NO_DATA_SETTLE_SEC)

        return "timeout", ""

    async def _wait_for_scoreline_ready(self, page, school_label, worker_id):
        try:
            await page.wait_for_selector(
                SCORELINE_SELECTOR, timeout=SCORELINE_READY_TIMEOUT_MS
            )
        except PlaywrightTimeoutError:
            print(
                f"[W{worker_id}] 【失败】未找到专业分数线模块: {school_label}"
            )
            return None

        scoreline = page.locator(SCORELINE_SELECTOR).first
        if not await scoreline.is_visible():
            print(
                f"[W{worker_id}] 【失败】专业分数线模块不可见: {school_label}"
            )
            return None

        filter_bar = await self._resolve_filter_bar(scoreline)
        try:
            await filter_bar.wait_for(state="visible", timeout=5000)
        except PlaywrightTimeoutError:
            is_no_enrollment, message = await self._detect_no_enrollment_state(
                scoreline
            )
            if is_no_enrollment:
                print(
                    f"[W{worker_id}] 【提示】未显示筛选栏，但检测到未招生："
                    f"{message} [{school_label}]"
                )
                return scoreline

            print(
                f"[W{worker_id}] 【失败】专业分数线筛选栏未出现: {school_label}"
            )
            return None

        return scoreline

    async def _wait_for_filter_value(
        self, page, scoreline, field_name, expected_value, school_label, worker_id=0
    ):
        if field_name not in FILTER_FIELD_ORDER:
            return False, "load", f"未知筛选字段 {field_name}", {}

        value_locator = await self._get_filter_value_locator(scoreline, field_name)

        visible = False
        try:
            await value_locator.wait_for(state="visible", timeout=5000)
            visible = True
        except PlaywrightTimeoutError:
            if await self._dismiss_blocking_modals(page, school_label, worker_id):
                try:
                    await value_locator.wait_for(state="visible", timeout=5000)
                    visible = True
                except PlaywrightTimeoutError:
                    pass

        if not visible:
            is_no_enrollment, message = await self._detect_no_enrollment_state(
                scoreline
            )
            if is_no_enrollment:
                filters = await self._get_scoreline_filters(scoreline)
                self._log_filters(
                    filters, f"已显示未招生空状态({message})", school_label
                )
                return True, "", "", filters

            filters = await self._get_scoreline_filters(scoreline)
            self._log_filters(filters, f"{field_name}筛选框未出现", school_label)
            return False, "load", f"{field_name}筛选框未出现", filters

        for poll_index in range(FILTER_VALUE_POLL_COUNT):
            if poll_index % 5 == 0:
                await self._dismiss_blocking_modals(page, school_label, worker_id)

            filters = await self._get_scoreline_filters(scoreline)
            actual = str(filters.get(field_name, "")).strip()
            if actual == expected_value:
                self._log_filters(filters, f"{field_name}就绪", school_label)
                return True, "", "", filters

            if await self._has_table_rows(scoreline):
                self._log_filters(
                    filters, f"{field_name}未就绪但列表已有数据", school_label
                )
                return True, "", "", filters

            is_no_enrollment, _ = await self._detect_no_enrollment_state(scoreline)
            if is_no_enrollment:
                self._log_filters(
                    filters, f"{field_name}未就绪但已显示未招生", school_label
                )
                return True, "", "", filters

            await asyncio.sleep(FILTER_VALUE_POLL_INTERVAL)

        filters = await self._get_scoreline_filters(scoreline)
        actual = str(filters.get(field_name, "")).strip()
        if actual == expected_value:
            self._log_filters(filters, f"{field_name}就绪", school_label)
            return True, "", "", filters

        if await self._has_table_rows(scoreline):
            self._log_filters(
                filters, f"{field_name}超时但列表已有数据", school_label
            )
            return True, "", "", filters

        is_no_enrollment, message = await self._detect_no_enrollment_state(scoreline)
        if is_no_enrollment:
            self._log_filters(
                filters, f"超时但已显示未招生({message})", school_label
            )
            return True, "", "", filters

        if actual and actual != expected_value:
            self._log_filters(filters, f"{field_name}无效", school_label)
            return False, "invalid", f"{field_name}={actual}", filters

        self._log_filters(filters, f"{field_name}值未就绪", school_label)
        return False, "load", f"{field_name}值未就绪", filters

    async def _wait_for_filter_has_value(
        self, page, scoreline, field_name, school_label, worker_id=0
    ):
        if field_name not in FILTER_FIELD_ORDER:
            return False, "load", f"未知筛选字段 {field_name}", {}

        value_locator = await self._get_filter_value_locator(scoreline, field_name)

        visible = False
        try:
            await value_locator.wait_for(state="visible", timeout=5000)
            visible = True
        except PlaywrightTimeoutError:
            if await self._dismiss_blocking_modals(page, school_label, worker_id):
                try:
                    await value_locator.wait_for(state="visible", timeout=5000)
                    visible = True
                except PlaywrightTimeoutError:
                    pass

        if not visible:
            is_no_enrollment, message = await self._detect_no_enrollment_state(
                scoreline
            )
            if is_no_enrollment:
                filters = await self._get_scoreline_filters(scoreline)
                self._log_filters(
                    filters, f"已显示未招生空状态({message})", school_label
                )
                return True, "", "", filters

            filters = await self._get_scoreline_filters(scoreline)
            self._log_filters(filters, f"{field_name}筛选框未出现", school_label)
            return False, "load", f"{field_name}筛选框未出现", filters

        for poll_index in range(FILTER_VALUE_POLL_COUNT):
            if poll_index % 5 == 0:
                await self._dismiss_blocking_modals(page, school_label, worker_id)

            filters = await self._get_scoreline_filters(scoreline)
            actual = str(filters.get(field_name, "")).strip()
            if actual:
                self._log_filters(filters, f"{field_name}有值", school_label)
                return True, "", "", filters

            is_no_enrollment, _ = await self._detect_no_enrollment_state(scoreline)
            if is_no_enrollment:
                self._log_filters(
                    filters, f"{field_name}未就绪但已显示未招生", school_label
                )
                return True, "", "", filters

            await asyncio.sleep(FILTER_VALUE_POLL_INTERVAL)

        filters = await self._get_scoreline_filters(scoreline)
        is_no_enrollment, message = await self._detect_no_enrollment_state(scoreline)
        if is_no_enrollment:
            self._log_filters(
                filters, f"超时但已显示未招生({message})", school_label
            )
            return True, "", "", filters

        self._log_filters(filters, f"{field_name}值未就绪", school_label)
        return False, "load", f"{field_name}值为空", filters

    async def _wait_for_scoreline_filters(
        self, page, scoreline, school_label, worker_id=0
    ):
        is_no_enrollment, message = await self._detect_no_enrollment_state(scoreline)
        if is_no_enrollment:
            return True, "no_enrollment", message, {}

        for attempt in range(2):
            await self._dismiss_blocking_modals(page, school_label, worker_id)

            province_ready, province_error_type, province_error, filters = (
                await self._wait_for_filter_value(
                    page,
                    scoreline,
                    "省份",
                    self.required_filter_values["省份"],
                    school_label,
                    worker_id,
                )
            )
            if not province_ready:
                if attempt == 0 and province_error_type == "load":
                    continue
                return False, province_error_type, province_error, filters

            year_ready, year_error_type, year_error, filters = (
                await self._wait_for_filter_value(
                    page,
                    scoreline,
                    "年份",
                    self.required_filter_values["年份"],
                    school_label,
                    worker_id,
                )
            )
            if not year_ready:
                if attempt == 0 and year_error_type == "load":
                    continue
                return False, year_error_type, year_error, filters

            for field_name in VALUE_ONLY_FILTERS:
                value_ready, value_error_type, value_error, filters = (
                    await self._wait_for_filter_has_value(
                        page, scoreline, field_name, school_label, worker_id
                    )
                )
                if not value_ready:
                    if attempt == 0 and value_error_type == "load":
                        break
                    return False, value_error_type, value_error, filters
            else:
                filters = await self._get_scoreline_filters(scoreline)
                self._log_filters(filters, "筛选就绪", school_label)
                return True, "", "", filters

            if attempt == 1:
                return False, value_error_type, value_error, filters

        filters = await self._get_scoreline_filters(scoreline)
        return False, "load", "筛选未就绪", filters

    async def _resolve_pagination_box(self, scoreline):
        pagination_box = scoreline.locator(PAGINATION_BOX_SELECTOR).first
        try:
            if await pagination_box.count() > 0 and await pagination_box.is_visible():
                return pagination_box
        except Exception:
            pass

        pagination_root = scoreline.locator(".ant-pagination").first
        try:
            if await pagination_root.count() > 0 and await pagination_root.is_visible():
                return pagination_root
        except Exception:
            pass

        return None

    async def _get_pagination_summary(self, scoreline):
        pagination_box = await self._resolve_pagination_box(scoreline)
        if pagination_box is None:
            return {
                "visible_page_count": 1,
                "has_next_page": False,
                "active_page": 1,
            }

        page_items = pagination_box.locator(PAGINATION_ITEM_SELECTOR)
        page_count = await page_items.count()
        active_page = 1
        for index in range(page_count):
            item = page_items.nth(index)
            class_name = (await item.get_attribute("class")) or ""
            if "ant-pagination-item-active" in class_name:
                title = await item.get_attribute("title")
                if title and str(title).isdigit():
                    active_page = int(title)
                break

        next_btn = pagination_box.locator(PAGINATION_NEXT_SELECTOR).first
        has_next_page = (
            await next_btn.count() > 0 and await next_btn.is_visible()
        )

        return {
            "visible_page_count": max(page_count, 1),
            "has_next_page": has_next_page,
            "active_page": active_page,
        }

    async def _has_next_page(self, scoreline):
        summary = await self._get_pagination_summary(scoreline)
        return summary["has_next_page"]

    async def _click_next_page(self, scoreline, school_label="", worker_id=0):
        await self._dismiss_blocking_modals(
            scoreline.page, school_label, worker_id
        )

        pagination_box = await self._resolve_pagination_box(scoreline)
        if pagination_box is None:
            return False

        next_btn = pagination_box.locator(PAGINATION_NEXT_SELECTOR).first
        if await next_btn.count() == 0 or not await next_btn.is_visible():
            return False

        previous_fingerprint = await self._get_table_fingerprint(scoreline)
        await next_btn.click()
        await self._dismiss_blocking_modals(
            scoreline.page, school_label, worker_id
        )
        return await self._wait_for_table_change(
            scoreline, previous_fingerprint, PAGE_CHANGE_TIMEOUT_MS
        )

    async def _parse_table_row(self, row_locator):
        major_cell = row_locator.locator("td").first
        if await major_cell.count() == 0:
            return None

        major_name = await self._get_locator_text(
            major_cell.locator("h3")
        )
        if not major_name:
            return None

        remark = await self._get_locator_text(major_cell.locator("p"))
        subject_text = await self._get_locator_text(
            major_cell.locator(".score-plan_xkyq__3FWHG")
        )
        score_text = await self._get_locator_text(row_locator.locator("td").nth(1))
        lowest_score, lowest_rank = self._parse_score_rank(score_text)

        return {
            "major_name": major_name,
            "remark": remark,
            "subject_requirement": self._parse_subject_requirement(subject_text),
            "lowest_score": lowest_score,
            "lowest_rank": lowest_rank,
        }

    async def _scrape_current_table_rows(self, scoreline):
        rows = scoreline.locator(TABLE_ROW_SELECTOR)
        row_count = await rows.count()
        parsed_rows = []

        for index in range(row_count):
            try:
                parsed_row = await self._parse_table_row(rows.nth(index))
                if parsed_row:
                    parsed_rows.append(parsed_row)
            except Exception as row_err:
                print(f"解析专业单行出错，跳过: {row_err}")
                continue

        return parsed_rows

    def _build_result_row(
        self,
        parsed_row,
        filters,
        school_id,
        school_name,
        school_profile,
    ):
        remark = parsed_row["remark"]
        major_display = merge_major_with_remark(parsed_row["major_name"], remark)

        return [
            school_id,
            school_name,
            school_profile.get("page_school_name", ""),
            school_profile.get("page_address", ""),
            school_profile.get("nature", ""),
            school_profile.get("school_type", ""),
            school_profile.get("department", ""),
            filters.get("省份", ""),
            filters.get("批次", ""),
            filters.get("科类", ""),
            parsed_row["subject_requirement"],
            major_display,
            parsed_row["lowest_score"],
            parsed_row["lowest_rank"],
            "",
            "",
            remark,
        ]

    async def _scrape_all_pages(
        self,
        scoreline,
        filters,
        school_id,
        school_name,
        school_profile,
        school_label="",
        worker_id=0,
    ):
        content_state, content_message = await self._wait_for_scoreline_content(
            scoreline
        )
        if content_state == "no_enrollment":
            print(
                f"[W{worker_id}] 【本省未招生】检测到空状态提示："
                f"{content_message} [{school_label}]"
            )
            return [], content_message

        if content_state == "timeout":
            is_no_enrollment, message = await self._detect_no_enrollment_state(
                scoreline
            )
            if is_no_enrollment:
                print(
                    f"[W{worker_id}] 【本省未招生】检测到空状态提示："
                    f"{message} [{school_label}]"
                )
                return [], message
            return [], ""

        pagination_summary = await self._get_pagination_summary(scoreline)
        print(
            f"[W{worker_id}] 【分页】筛选 {filters.get('省份', '')}/"
            f"{filters.get('年份', '')}/{filters.get('批次', '')}/"
            f"{filters.get('科类', '')} | 当前第 "
            f"{pagination_summary['active_page']} 页 | "
            f"可见页码 {pagination_summary['visible_page_count']} 个 | "
            f"{'有下一页' if pagination_summary['has_next_page'] else '仅单页'} "
            f"[{school_label}]"
        )

        result_rows = []
        page_index = 1

        while page_index <= MAX_PAGINATION_PAGES:
            parsed_rows = await self._scrape_current_table_rows(scoreline)
            print(
                f"[W{worker_id}] 【分页】第 {page_index} 页解析 "
                f"{len(parsed_rows)} 条专业 [{school_label}]"
            )

            for parsed_row in parsed_rows:
                result_rows.append(
                    self._build_result_row(
                        parsed_row,
                        filters,
                        school_id,
                        school_name,
                        school_profile,
                    )
                )

            if not await self._has_next_page(scoreline):
                break

            print(
                f"[W{worker_id}] 【分页】第 {page_index} 页完成，"
                f"点击下一页... [{school_label}]"
            )
            moved = await self._click_next_page(scoreline, school_label, worker_id)
            if not moved:
                print(
                    f"[W{worker_id}] 【警告】下一页切换失败或表格未变化，"
                    f"停止翻页 [{school_label}]"
                )
                break

            page_index += 1

        if page_index > 1 or pagination_summary["has_next_page"]:
            print(
                f"[W{worker_id}] 【分页】翻页结束，共抓取 {page_index} 页，"
                f"累计 {len(result_rows)} 条专业 [{school_label}]"
            )

        return result_rows, ""

    async def _get_page_school_profile(
        self, page, school_label="", worker_id=0, timeout=SCHOOL_PROFILE_TIMEOUT_MS
    ):
        await self._dismiss_blocking_modals(page, school_label, worker_id)

        info = page.locator(SCHOOL_INFO_SELECTOR).first
        name_locator = page.locator(SCHOOL_NAME_SELECTOR).first

        try:
            await name_locator.wait_for(state="visible", timeout=timeout)
        except PlaywrightTimeoutError:
            return None

        name = (await name_locator.inner_text()).strip()
        if not name:
            return None

        address = ""
        if await info.count() > 0:
            address_locator = info.locator(SCHOOL_ADDRESS_SELECTOR).first
            if await address_locator.count() > 0:
                address = (await address_locator.inner_text()).strip()

            core_tags = []
            core_locator = info.locator(SCHOOL_CORE_TAGS_SELECTOR)
            core_count = await core_locator.count()
            for index in range(core_count):
                tag_text = (await core_locator.nth(index).inner_text()).strip()
                if tag_text:
                    core_tags.append(tag_text)
        else:
            core_tags = []

        return {
            "name": name,
            "address": address,
            "parsed_core": self._parse_page_core_tags(core_tags),
        }

    async def _scrape_school(self, page, row_index, school_meta, worker_id):
        school_id = school_meta["school_id"]
        school_name = school_meta["school_name"]
        school_label = (
            f"{school_name}({school_id})" if school_name else f"ID={school_id}"
        )
        url = self._build_url(school_id)
        result_text = "失败"

        try:
            await page.bring_to_front()
            await asyncio.sleep(random.uniform(0.2, 0.5))
            await page.goto(url, wait_until="domcontentloaded")
            await self._dismiss_blocking_modals(page, school_label, worker_id)

            current_url = page.url.split("?")[0].rstrip("/")
            expected_url = url.rstrip("/")
            if current_url != expected_url:
                print(
                    f"[W{worker_id}] 【无效】页面被重定向: {url} -> {current_url}"
                )
                self._save_status(row_index, "无效", "ID不存在或页面跳转")
                result_text = "无效"
                return

            page_profile = await self._get_page_school_profile(
                page, school_label, worker_id
            )
            if page_profile is None:
                print(
                    f"[W{worker_id}] 【失败】未读取到院校标题信息: {school_label}"
                )
                self._save_status(row_index, "失败-未加载", "未找到院校标题")
                result_text = "失败-未加载"
                return

            school_profile = self._merge_school_profile(page_profile, school_meta)

            scoreline = await self._wait_for_scoreline_ready(
                page, school_label, worker_id
            )
            if scoreline is None:
                self._save_status(row_index, "失败-未加载", "专业分数线模块未出现")
                result_text = "失败-未加载"
                return

            is_no_enrollment, no_enrollment_message = (
                await self._detect_no_enrollment_state(scoreline)
            )
            if is_no_enrollment:
                print(
                    f"[W{worker_id}] 【本省未招生】{no_enrollment_message}: "
                    f"{school_label}"
                )
                self._save_status(row_index, "本省未招生", no_enrollment_message)
                result_text = "本省未招生"
                return

            filters_ready, error_type, filter_error, filters = (
                await self._wait_for_scoreline_filters(
                    page, scoreline, school_label, worker_id
                )
            )
            if not filters_ready:
                if error_type == "invalid":
                    print(
                        f"[W{worker_id}] 【无效数据】{filter_error}: {school_label}"
                    )
                    self._save_status(row_index, "无效数据", filter_error)
                    result_text = "无效数据"
                else:
                    print(
                        f"[W{worker_id}] 【失败】{filter_error}: {school_label}"
                    )
                    self._save_status(row_index, "失败-未加载", filter_error)
                    result_text = "失败-未加载"
                return

            filter_ok, filter_key, expected, actual = self._validate_filters(filters)
            if not filter_ok:
                self._log_filters(filters, "校验失败", school_label)
                actual_text = str(actual).strip()
                has_table_data = await self._has_table_rows(scoreline)

                if has_table_data:
                    print(
                        f"[W{worker_id}] 【警告】「{filter_key}」"
                        f"期望「{expected}」实际「{actual_text}」，"
                        f"但列表已有数据，继续抓取: {school_label}"
                    )
                else:
                    print(
                        f"[W{worker_id}] 【无效数据】「{filter_key}」"
                        f"期望「{expected}」实际「{actual_text}」: {school_label}"
                    )
                    self._save_status(
                        row_index,
                        "无效数据",
                        f"{filter_key}={actual_text}",
                    )
                    result_text = "无效数据"
                    return

            print(
                f"[W{worker_id}] 【抓取】{filters['省份']} {filters['年份']} "
                f"{filters['批次']} {filters['科类']} | "
                f"页面院校: {school_profile['page_school_name']} | "
                f"{school_profile['nature']}/{school_profile['school_type']}/"
                f"{school_profile['department']} | {school_label}"
            )

            result_rows, no_enrollment_message = await self._scrape_all_pages(
                scoreline,
                filters,
                school_meta["school_id"],
                school_meta["school_name"],
                school_profile,
                school_label,
                worker_id,
            )
            self._append_result_rows(result_rows)

            if result_rows:
                self._save_status(row_index, "成功")
                result_text = f"成功 {len(result_rows)} 条专业"
                print(
                    f"[W{worker_id}] 【成功】{school_label} "
                    f"共 {len(result_rows)} 条专业"
                )
            elif no_enrollment_message:
                print(
                    f"[W{worker_id}] 【本省未招生】"
                    f"{self.target_province}/{self.target_year} "
                    f"{no_enrollment_message}: {school_label}"
                )
                self._save_status(row_index, "本省未招生", no_enrollment_message)
                result_text = "本省未招生"
            else:
                print(
                    f"[W{worker_id}] 【本省未招生】"
                    f"{self.target_province}/{self.target_year} 无专业数据: "
                    f"{school_label}"
                )
                self._save_status(row_index, "本省未招生", "无专业数据")
                result_text = "本省未招生"

        except Exception as err:
            self._save_status(row_index, "失败", str(err))
            result_text = f"异常: {err}"
            print(
                f"[W{worker_id}] 异常 | 行号{row_index} | "
                f"{school_label} | {err}"
            )
        finally:
            self._mark_task_finished(school_label, worker_id, result_text)

    async def _init_worker_pages(self, context, login_page):
        worker_pages = []

        for worker_id in range(self.concurrent_workers):
            if worker_id == 0:
                page = login_page
            else:
                page = await context.new_page()
                await page.goto(LOGIN_URL, wait_until="domcontentloaded")
                await page.evaluate(
                    f"document.title = '[Worker {worker_id + 1}] 掌上高考'"
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
                row_index, school_meta = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            self._mark_task_started(
                school_meta["school_name"] or f"ID={school_meta['school_id']}",
                worker_id,
            )
            await self._scrape_school(page, row_index, school_meta, worker_id)
            await asyncio.sleep(random.uniform(0.8, 1.5))

    async def _run_async(self):
        start_time = time.time()

        self._init_source_table()
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
                elif status in {"无效", "无效数据"}:
                    invalid_count += 1
                elif status in {"本省未招生", "失败-无数据"}:
                    no_enrollment_count += 1
                continue

            school_meta = self._get_school_meta(index)
            if not school_meta["school_id"]:
                continue

            pending_tasks.append((index, school_meta))

        self._total_pending = len(pending_tasks)
        self._skip_count = skip_count
        self._started_count = 0
        self._finished_count = 0

        if not pending_tasks:
            print(
                f"没有待爬取的院校。"
                f"总计 {self._total_schools} 所，已成功 {success_count}，"
                f"无效 {invalid_count}，本省未招生 {no_enrollment_count}，"
                f"跳过 {invalid_count + no_enrollment_count} 所。"
            )
            return

        mode_text = (
            f"并发 {self.concurrent_workers} 标签页"
            if self.enable_concurrent
            else "单标签页顺序执行"
        )
        print(
            f"\n>>>> 目标 {self.target_province}/{self.target_year}/"
            f"{self.target_subject}/{self.target_batch} | "
            f"结果表 {os.path.basename(self.result_path)}"
        )
        print(
            f">>>> 院校总计 {self._total_schools} 所 | "
            f"已成功 {success_count} | 无效 {invalid_count} | "
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

        profile_dir = get_profile_dir(
            os.path.dirname(os.path.abspath(self.school_source_path))
        )
        os.makedirs(profile_dir, exist_ok=True)

        try:
            async with async_playwright() as playwright:
                context = await playwright.chromium.launch_persistent_context(
                    user_data_dir=profile_dir,
                    headless=False,
                    user_agent=USER_AGENT,
                    ignore_https_errors=True,
                    viewport={"width": 1280, "height": 900},
                )

                login_page = (
                    context.pages[0] if context.pages else await context.new_page()
                )
                print("=" * 56)
                print("浏览器已打开（有头模式）。")
                print("请在浏览器中完成登录（扫码 / 账号均可）。")
                print("登录完成后，回到此终端按【回车键】继续爬取...")
                print("=" * 56)
                await login_page.goto(LOGIN_URL, wait_until="domcontentloaded")
                await login_page.evaluate("document.title = '[Worker 1] 掌上高考'")
                await asyncio.to_thread(input)

                print(f"\n正在打开 {self.concurrent_workers} 个并发标签页...")
                worker_pages = await self._init_worker_pages(context, login_page)

                queue = asyncio.Queue()
                for task in pending_tasks:
                    await queue.put(task)

                workers = [
                    asyncio.create_task(
                        self._worker(worker_id + 1, worker_pages[worker_id], queue)
                    )
                    for worker_id in range(self.concurrent_workers)
                ]
                await asyncio.gather(*workers)

                for page in worker_pages[1:]:
                    if not page.is_closed():
                        await page.close()

                await context.close()
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

    school_source_path = os.path.join(current_dir, "普通高校_带id.csv")
    status_save_path = os.path.join(current_dir, "普通高校_带id.csv")

    scraper = GaokaoScorelineScraper(
        school_source_path=school_source_path,
        status_save_path=status_save_path,
        target_province=TARGET_PROVINCE,
        target_year=TARGET_YEAR,
        target_batch=TARGET_BATCH,
        target_subject=TARGET_SUBJECT,
        enable_concurrent=ENABLE_CONCURRENT,
        concurrent_workers=CONCURRENT_WORKERS,
    )
    scraper.run()
