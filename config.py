from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple


@dataclass(frozen=True)
class PathsConfig:
    input_excel: str = "audit_samples.xlsx"
    output_excel: str = "audit_report.xlsx"
    input_sheet_preferred: str = "raw"


@dataclass(frozen=True)
class ColumnsConfig:
    """
    内部标准列名（canonical）：
      id, news_raw, country, decision, triggered_keywords, rule_hit, human_check
    可选：source, key_content, human_decision

    COLUMN_MAPPING 规则：
      key = 内部标准列名
      value = 输入表实际列名
    """
    column_mapping: Dict[str, str] = None

    required_cols: List[str] = None  # 必填（canonical）
    allowed_decisions: Tuple[str, ...] = ("删除", "人审", "通过")
    allowed_human_check: Tuple[str, ...] = ("TRUE", "FALSE")


@dataclass(frozen=True)
class KeywordRiskConfig:
    high_risk_min_total_hits: int = 3
    high_risk_min_bad_rate: float = 0.5
    topn_keywords: int = 20


@dataclass(frozen=True)
class SliceConfig:
    """
    长度分桶：按 news_raw 字符数
    默认 buckets: <80, 80-150, 150-300, >300
    格式为区间上界（含）：
      (80, "<80"), (150, "80-150"), (300, "150-300"), (10**9, ">300")
    """
    length_buckets: Tuple[Tuple[int, str], ...] = (
        (79, "<80"),
        (150, "80-150"),
        (300, "150-300"),
        (10**9, ">300"),
    )


@dataclass(frozen=True)
class StyleConfig:
    # cleaned_raw 单元格颜色
    invalid_fill_hex: str = "FFF4CCCC"   # 浅红
    duplicate_id_fill_hex: str = "FFFFE5CC"  # 浅橙


@dataclass(frozen=True)
class AppConfig:
    paths: PathsConfig = PathsConfig()
    columns: ColumnsConfig = ColumnsConfig(
        column_mapping={
            "id": "id",
            "news_raw": "news_raw",
            "country": "country",
            "decision": "decision",
            "triggered_keywords": "triggered_keywords",
            "rule_hit": "rule_hit",
            "human_check": "human_check",
            # optional:
            "source": "source",
            "key_content": "key_content",
            "human_decision": "human_decision",
        },
        required_cols=["id", "news_raw", "country", "decision", "triggered_keywords", "human_check"],
    )
    keyword_risk: KeywordRiskConfig = KeywordRiskConfig()
    slice_cfg: SliceConfig = SliceConfig()
    style: StyleConfig = StyleConfig()


CONFIG = AppConfig()
