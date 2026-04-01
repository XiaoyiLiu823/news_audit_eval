from __future__ import annotations

import ast
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from config import AppConfig, CONFIG

logger = logging.getLogger(__name__)


CANONICAL_COLS = [
    "id",
    "news_raw",
    "country",
    "decision",
    "triggered_keywords",
    "rule_hit",
    "human_check",
    "source",
    "key_content",
    "human_decision",
]


@dataclass
class QualityIssue:
    """用于导出 Excel 时做单元格上色"""
    excel_row_1based: int  # 对应 openpyxl 的行号（含表头，从1开始）
    col_name: str
    issue_type: str  # "invalid" / "duplicate_id"


@dataclass
class EvalOutputs:
    cleaned_raw_df: pd.DataFrame
    quality_issues_df: pd.DataFrame
    overview_tables: Dict[str, pd.DataFrame]
    accuracy_breakdown_df: pd.DataFrame
    confusion_payload: Dict[str, Any]  # {mode: "available"/"unavailable", tables...}
    top_wrong_cases_df: pd.DataFrame
    keyword_risk_tables: Dict[str, pd.DataFrame]
    slice_tables: Dict[str, pd.DataFrame]
    suggestions_df: pd.DataFrame
    issues_to_style: List[QualityIssue]
    meta: Dict[str, Any]  # logs/summary numbers


def read_input_excel(cfg: AppConfig) -> Tuple[pd.DataFrame, str]:
    path = cfg.paths.input_excel
    try:
        xls = pd.ExcelFile(path)
    except FileNotFoundError:
        raise FileNotFoundError(f"输入文件不存在：{path}")

    sheet_names = xls.sheet_names
    preferred = cfg.paths。input_sheet_preferred

    if preferred in sheet_names:
        sheet = preferred
    else:
        sheet = sheet_names[0]
        logger.warning("未找到 sheet=%s，将读取第一个 sheet=%s", preferred, sheet)

    df = pd.read_excel(path, sheet_name=sheet, dtype=object)
    logger.info("已读取输入：%s | sheet=%s | rows=%d cols=%d", path, sheet, len(df), df.shape[1])
    return df, sheet


def apply_column_mapping(df: pd.DataFrame, cfg: AppConfig) -> pd.DataFrame:
    mapping = cfg.columns.column_mapping

    # 输入列 -> canonical
    inverse: Dict[str, str] = {}
    for canon, src in mapping.items():
        if src in df.columns:
            inverse[src] = canon

    df2 = df.rename(columns=inverse).copy()

    # 确保所有 canonical 列存在（不存在则创建空列）
    for c in CANONICAL_COLS:
        if c not in df2.columns:
            df2[c] = np.nan

    # 检查必填列是否至少存在（canonical）
    missing_required = [c for c in cfg.columns.required_cols if c not in df2.columns]
    if missing_required:
        raise ValueError(f"缺少必填列（映射后）：{missing_required}")

    return df2


def _is_blank(v: Any) -> bool:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return True
    if isinstance(v, str) and v.strip() == "":
        return True
    return False


def normalize_human_check(v: Any) -> Optional[str]:
    """
    归一化 human_check 到 {"TRUE","FALSE"}，返回 None 表示非法/缺失
    支持：TRUE/FALSE/True/False/1/0 等（尽量稳健）
    """
    if _is_blank(v):
        return None

    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"

    s = str(v).strip()
    if s == "":
        return None
    s_upper = s.upper()

    if s_upper in ("TRUE", "T", "YES", "Y", "1"):
        return "TRUE"
    if s_upper in ("FALSE", "F", "NO", "N", "0"):
        return "FALSE"

    # 兼容 "正确/错误" 这种可选（不写进要求，但尽量兜底）
    if s in ("正确", "对", "是"):
        return "TRUE"
    if s in ("错误", "错", "否", "不"):
        return "FALSE"

    return None


def normalize_decision(v: Any, allowed: Tuple[str, ...]) -> Optional[str]:
    if _is_blank(v):
        return None
    s = str(v).strip()
    return s if s in allowed else None


def parse_keywords(v: Any) -> List[str]:
    """
    triggered_keywords 解析：
    - 可能是 "a,b,c"
    - 可能是 "a; b; c"
    - 可能是 "['a','b']" / '["a","b"]'
    - 可能混合空格
    输出：去重、去空、保序
    """
    if _is_blank(v):
        return []

    # 如果本身就是 list
    if isinstance(v, list):
        items = [str(x).strip() for x in v if str(x).strip()]
        return _dedup_keep_order(items)

    s = str(v).strip()
    if s == "":
        return []

    # 尝试解析 list-like 字符串
    if (s.startswith("[") and s.endswith("]")) or (s.startswith("(") and s.endswith(")")):
        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, (list, tuple)):
                items = [str(x).strip() for x in parsed if str(x).strip()]
                return _dedup_keep_order(items)
        except Exception:
            pass  # 解析失败则走常规分割

    # 常规分割：逗号/分号/中文分号/竖线/空格
    parts = re.split(r"[,;，；\|\s]+", s)
    parts = [p.strip() for p in parts if p and p.strip()]
    return _dedup_keep_order(parts)


def _dedup_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def compute_quality_flags(df: pd.DataFrame, cfg: AppConfig) -> Tuple[pd.DataFrame, List[QualityIssue], pd.DataFrame]:
    """
    返回：
    - cleaned_raw_df（新增 bad_row/bad_reason/normalized 列）
    - issues_to_style（用于 Excel 上色）
    - quality_issues_df（坏行原因分布）
    """
    required = cfg.columns.required_cols
    allowed_decisions = cfg.columns.allowed_decisions

    df2 = df.copy()

    # 归一化列
    df2["human_check_norm"] = df2["human_check"].apply(normalize_human_check)
    df2["decision_norm"] = df2["decision"].apply(lambda x: normalize_decision(x, allowed_decisions))
    df2["triggered_keywords_list"] = df2["triggered_keywords"].apply(parse_keywords)
    df2["triggered_keywords_norm"] = df2["triggered_keywords_list"].apply(lambda xs: ",".join(xs))

    # id 处理（缺失/非法）
    def to_int_or_none(x: Any) -> Optional[int]:
        if _is_blank(x):
            return None
        try:
            # 支持 "1.0"
            return int(float(str(x).strip()))
        except Exception:
            return None

    df2["id_norm"] = df2["id"].apply(to_int_or_none)

    issues: List[QualityIssue] = []
    bad_reasons: List[str] = []
    bad_flags: List[bool] = []

    # 先算重复 id（只针对 id_norm 非空）
    id_series = df2["id_norm"]
    dup_mask = id_series.notna() & id_series.duplicated(keep=False)

    # 遍历行做 bad_row/bad_reason + 收集上色单元格
    for idx, row in df2.iterrows():
        reasons: List[str] = []

        excel_row = idx + 2  # 1-based + header

        # 必填列空值检查（按 canonical 必填列）
        for col in required:
            if col == "human_check":
                # 用 human_check_norm 判断
                if row.get("human_check_norm") is None:
                    reasons.append("human_check_missing_or_illegal")
                    issues.append(QualityIssue(excel_row, "human_check", "invalid"))
            elif col == "decision":
                if row.get("decision_norm") is None:
                    if _is_blank(row.get("decision")):
                        reasons.append("decision_missing")
                    else:
                        reasons.append("decision_illegal")
                    issues.append(QualityIssue(excel_row, "decision", "invalid"))
            elif col == "triggered_keywords":
                # triggered_keywords 允许为空？你的要求是必填列为空视为坏行
                if _is_blank(row.get("triggered_keywords")):
                    reasons.append("triggered_keywords_missing")
                    issues.append(QualityIssue(excel_row, "triggered_keywords", "invalid"))
            elif col == "id":
                if row.get("id_norm") is None:
                    reasons.append("id_missing_or_illegal")
                    issues.append(QualityIssue(excel_row, "id", "invalid"))
            else:
                if _is_blank(row.get(col)):
                    reasons.append(f"{col}_missing")
                    issues.append(QualityIssue(excel_row, col, "invalid"))

        # 重复 id
        if row.get("id_norm") is not None and bool(dup_mask.loc[idx]):
            reasons.append("id_duplicate")
            issues.append(QualityIssue(excel_row, "id", "duplicate_id"))

        # 合并
        bad = len(reasons) > 0
        bad_flags.append(bad)
        bad_reasons.append(";".join(_dedup_keep_order(reasons)))

    df2["bad_row"] = bad_flags
    df2["bad_reason"] = bad_reasons

    # 输出 cleaned_raw：尽量保留原始列 + 增加列（按你要求）
    cleaned_cols_order = [
        "id", "id_norm",
        "news_raw",
        "country",
        "decision", "decision_norm",
        "triggered_keywords", "triggered_keywords_norm",
        "rule_hit",
        "human_check", "human_check_norm",
        "source",
        "key_content",
        "human_decision",
        "bad_row", "bad_reason",
    ]
    for c in cleaned_cols_order:
        if c not in df2.columns:
            df2[c] = np.nan
    cleaned_raw_df = df2[cleaned_cols_order].copy()

    # 质量问题统计
    reason_counts = (
        cleaned_raw_df.loc[cleaned_raw_df["bad_row"] == True, "bad_reason"]
        。fillna("")
        。astype(str)
    )

    reason_counter: Dict[str, int] = {}
    for s in reason_counts:
        if not s:
            continue
        for r in s.split(";"):
            r = r.strip()
            if not r:
                continue
            reason_counter[r] = reason_counter.get(r, 0) + 1

    quality_issues_df = pd.DataFrame(
        [{"issue": k, "count": v} for k, v in sorted(reason_counter.items(), key=lambda x: (-x[1], x[0]))]
    )
    quality_issues_df["percent_of_bad_rows"] = (
        quality_issues_df["count"] / max(1, int(cleaned_raw_df["bad_row"].sum()))
    ).round(4)

    return cleaned_raw_df, issues, quality_issues_df


def country_distribution(df_valid: pd.DataFrame) -> pd.DataFrame:
    s = df_valid["country"].fillna("未识别").astype(str)
    cnt = s.value_counts(dropna=False)
    out = pd.DataFrame({"country": cnt.index, "count": cnt.values})
    out["percent"] = (out["count"] / out["count"].sum()).round(4)
    return out


def decision_distribution(df_valid: pd.DataFrame) -> pd.DataFrame:
    s = df_valid["decision_norm"].fillna("非法/缺失").astype(str)
    cnt = s.value_counts(dropna=False)
    out = pd.DataFrame({"decision": cnt.index, "count": cnt.values})
    out["percent"] = (out["count"] / out["count"].sum()).round(4)
    return out


def unrecognized_ratio(df_valid: pd.DataFrame) -> pd.DataFrame:
    s = df_valid["country"].fillna("").astype(str).str.strip()
    mask = s.isin(["未识别", "UNKNOWN", "Unknown", "unknown"])
    total = len(df_valid)
    count = int(mask.sum())
    percent = round(count / total, 4) if total else 0.0
    return pd.DataFrame([{"metric": "unrecognized_country", "count": count, "percent": percent, "total": total}])


def country_decision_crosstab(df_valid: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    返回：
    - count 表：country × decision
    - row% 表：country × decision（按 country 行归一）
    """
    ct = pd.crosstab(df_valid["country"], df_valid["decision_norm"], dropna=False)
    ct = ct.reset_index().rename(columns={"decision_norm": "decision"})
    # row%
    ctn = pd.crosstab(df_valid["country"], df_valid["decision_norm"], normalize="index", dropna=False).round(4)
    ctn = ctn.reset_index()
    return ct, ctn


def accuracy_tables(df_valid: pd.DataFrame) -> pd.DataFrame:
    """
    输出按 overall / decision / country / rule_hit 的 TRUE/FALSE 分布与 accuracy
    永远返回 DataFrame（即使 df_valid 为空）
    """
    df = df_valid.copy()

    # 确保必要列存在
    for col in ["human_check_norm", "decision_norm", "country", "rule_hit"]:
        if col not in df.columns:
            df[col] = np.nan

    df["is_true"] = df["human_check_norm"] == "TRUE"

    cols = ["group_type", "group_value", "count", "true_count", "false_count", "accuracy"]

    def make_group(group_type: str, group_col: Optional[str]) -> pd.DataFrame:
        # overall
        if group_col is None:
            total = len(df)
            true_cnt = int(df["is_true"].sum()) if total else 0
            false_cnt = total - true_cnt
            acc = round(true_cnt / total, 4) if total else 0.0
            return pd.DataFrame([{
                "group_type": group_type,
                "group_value": "ALL",
                "count": total,
                "true_count": true_cnt,
                "false_count": false_cnt,
                "accuracy": acc,
            }], columns=cols)

        # 分组为空
        if df.empty:
            return pd.DataFrame(columns=cols)

        rows = []
        for val, g in df.groupby(group_col, dropna=False):
            total = len(g)
            true_cnt = int(g["is_true"].sum())
            false_cnt = total - true_cnt
            acc = round(true_cnt / total, 4) if total else 0.0
            rows.append({
                "group_type": group_type,
                "group_value": str(val),
                "count": total,
                "true_count": true_cnt,
                "false_count": false_cnt,
                "accuracy": acc,
            })

        out = pd.DataFrame(rows, columns=cols)
        if out.empty:
            return out
        return out.sort_values(["accuracy", "count"], ascending=[True, False])

    parts = [
        make_group("overall", None),
        make_group("decision", "decision_norm"),
        make_group("country", "country"),
        make_group("rule_hit", "rule_hit"),
    ]

    result = pd.concat(parts, ignore_index=True)
    # 再兜一次：确保列齐全
    for c in cols:
        if c not in result.columns:
            result[c] = np.nan
    return result[cols]


def confusion_or_fallback(df_valid: pd.DataFrame) -> Dict[str, Any]:
    """
    若存在 human_decision（且合法），输出混淆矩阵与误删/漏删；
    否则输出说明 + TRUE/FALSE×decision 的 2×3 表
    """
    payload: Dict[str, Any] = {}
    has_human_decision = "human_decision" in df_valid.columns and df_valid["human_decision"].notna().any()

    if not has_human_decision:
        # fallback
        tf_decision = pd.crosstab(df_valid["human_check_norm"], df_valid["decision_norm"], dropna=False)
        tf_decision = tf_decision.reset_index()
        payload["mode"] = "unavailable"
        payload["note"] = (
            "输入表不存在 human_decision 列（或全为空），无法严格计算混淆矩阵/误删率/漏删率。"
            "可在输入表增加 human_decision（删除/人审/通过）后自动输出。"
        )
        payload["tf_by_decision"] = tf_decision
        return payload

    # 归一化 human_decision
    allowed = {"删除", "人审", "通过"}
    hd = df_valid["human_decision"].astype(str).str.strip()
    valid_mask = hd.isin(list(allowed))
    df2 = df_valid.loc[valid_mask].copy()

    if df2.empty:
        tf_decision = pd.crosstab(df_valid["human_check_norm"], df_valid["decision_norm"], dropna=False).reset_index()
        payload["mode"] = "unavailable"
        payload["note"] = "human_decision 列存在但没有合法值（仅允许：删除/人审/通过），已回退输出 TRUE/FALSE×decision。"
        payload["tf_by_decision"] = tf_decision
        return payload

    cm = pd.crosstab(df2["human_decision"], df2["decision_norm"], dropna=False)

    # 误删率、漏删率定义
    # 误删：human=通过 & decision=删除 / human=通过 总数
    # 漏删：human=删除 & decision=通过 / human=删除 总数
    human_pass_total = int((df2["human_decision"] == "通过").sum())
    human_delete_total = int((df2["human_decision"] == "删除").sum())

    false_delete = int(((df2["human_decision"] == "通过") & (df2["decision_norm"] == "删除")).sum())
    missed_delete = int(((df2["human_decision"] == "删除") & (df2["decision_norm"] == "通过")).sum())

    false_delete_rate = round(false_delete / human_pass_total, 4) if human_pass_total else np.nan
    missed_delete_rate = round(missed_delete / human_delete_total, 4) if human_delete_total else np.nan

    payload["mode"] = "available"
    payload["confusion_matrix"] = cm.reset_index()
    payload["metrics"] = pd.DataFrame([{
        "metric": "false_delete_rate",
        "definition": "human_decision=通过 且 decision=删除 / human_decision=通过",
        "value": false_delete_rate,
        "numerator": false_delete,
        "denominator": human_pass_total,
    }, {
        "metric": "missed_delete_rate",
        "definition": "human_decision=删除 且 decision=通过 / human_decision=删除",
        "value": missed_delete_rate,
        "numerator": missed_delete,
        "denominator": human_delete_total,
    }])
    payload["note"] = "human_decision 可用，已输出混淆矩阵与误删/漏删率。"
    payload["rows_used_for_confusion"] = len(df2)
    payload["rows_dropped_invalid_human_decision"] = int((~valid_mask).sum())
    return payload


def generate_why_wrong(row: pd.Series) -> str:
    """
    why_wrong：保守模板推断（不编造事实），仅基于触发词/字段形态做原因猜测。
    """
    decision = str(row.get("decision_norm", "")).strip()
    country = str(row.get("country", "")).strip()
    rule_hit = str(row.get("rule_hit", "")).strip()
    kws = str(row.get("triggered_keywords_norm", "")).strip()
    text = str(row.get("news_raw", "")).strip()
    text_len = len(text)

    kw_list = [k for k in kws.split(",") if k.strip()] if kws else []
    short_kw = [k for k in kw_list if re.fullmatch(r"[A-Za-z]{1,3}\.?|U\.S\.", k) is not None]
    multi_kw = len(kw_list) >= 4

    if not kw_list:
        return "可能原因：未命中有效关键词或关键词字段为空，导致规则判定缺少依据/配置不一致。"
    if "UNKNOWN" in rule_hit or country in ("未识别", "UNKNOWN"):
        if decision == "人审":
            return "可能原因：主体未识别触发默认人审，但人工认为可直接通过/或应落到某国主体词表。"
        return "可能原因：主体未识别但仍给出强决策，建议检查默认分流逻辑或补充主体词表。"
    if short_kw:
        return "可能原因：英文缩写/短词（如 US/UN/EU 等）存在误命中或引用场景，建议加词边界/组合条件或白名单。"
    if country == "多国" or ("+" in rule_hit and "OTHER_RULE" in rule_hit):
        return "可能原因：多国共现导致优先级/覆盖策略触发，人工判断主体可能并非被优先级选中的国家。"
    if decision == "删除" and country != "美国":
        return "可能原因：删除类规则覆盖过宽或触发词歧义，建议收敛关键词或加上下文组合条件。"
    if decision == "通过" and ("SENSITIVE" in rule_hit or "SENSITIVE_RULE" in rule_hit):
        return "可能原因：敏感词/风险词被忽略或覆盖，建议确保敏感词提升为人审或增强组合策略。"
    if text_len < 60:
        return "可能原因：文本过短信息不足，关键词更易误触发；建议短文本单独策略或提高人审比例。"
    if multi_kw:
        return "可能原因：命中关键词较多但主体不清晰，可能存在引用/对比/转述导致误判，建议加入主体判定的组合约束。"
    return "可能原因：地名/机构名歧义或引用他国事件（主体非该国），建议对高频歧义词做白名单或组合规则。"


def top_wrong_cases(df_valid: pd.DataFrame, topn: int = 10) -> pd.DataFrame:
    wrong = df_valid.loc[df_valid["human_check_norm"] == "FALSE"].copy()
    if wrong.empty:
        return pd.DataFrame(columns=[
            "id_norm", "decision_norm", "country", "rule_hit", "triggered_keywords_norm", "news_raw_short", "why_wrong"
        ])

    wrong["news_raw_short"] = wrong["news_raw"].astype(str).str.slice(0, 200)
    wrong["why_wrong"] = wrong.apply(generate_why_wrong, axis=1)

    rows = []
    for decision, g in wrong.groupby("decision_norm", dropna=False):
        g2 = g.sort_values("id_norm", ascending=True).head(topn)
        rows.append(g2)

    out = pd.concat(rows, ignore_index=True) if rows else wrong.head(topn)
    out = out[[
        "id_norm",
        "decision_norm",
        "country",
        "rule_hit",
        "triggered_keywords_norm",
        "news_raw_short",
        "why_wrong",
    ]].rename(columns={
        "id_norm": "id",
        "decision_norm": "decision",
        "triggered_keywords_norm": "triggered_keywords",
    })

    # 按 decision 排序，方便索引
    out["decision"] = out["decision"].astype(str)
    return out.sort_values(["decision", "id"], ascending=[True, True]).reset_index(drop=True)


def keyword_risk(df_valid: pd.DataFrame, cfg: AppConfig) -> Dict[str, pd.DataFrame]:
    """
    输出：
    - keyword_stats：total_hits/bad_hits/bad_rate
    - high_risk_keywords：筛选后的 topN（按 bad_rate desc, total_hits desc）
    """
    kw_cfg = cfg.keyword_risk

    # 关键词总表
    records: List[Dict[str, Any]] = []
    for _, row in df_valid.iterrows():
        kws = parse_keywords(row.get("triggered_keywords"))
        is_bad = row.get("human_check_norm") == "FALSE"
        records.append({
            "kws": kws,
            "is_bad": is_bad,
        })

    total_counter: Dict[str, int] = {}
    bad_counter: Dict[str, int] = {}
    for r in records:
        kws = set(r["kws"])  # 每条去重计一次
        for k in kws:
            total_counter[k] = total_counter.get(k, 0) + 1
            if r["is_bad"]:
                bad_counter[k] = bad_counter.get(k, 0) + 1

    rows = []
    for k, total in total_counter.items():
        bad = bad_counter.get(k, 0)
        bad_rate = bad / total if total else 0.0
        rows.append({
            "keyword": k,
            "total_hits": total,
            "bad_hits": bad,
            "bad_rate": round(bad_rate, 4),
        })

    stats = pd.DataFrame(rows).sort_values(["bad_rate", "total_hits"], ascending=[False, False])

    high_risk = stats.loc[
        (stats["total_hits"] >= kw_cfg.high_risk_min_total_hits) &
        (stats["bad_rate"] >= kw_cfg.high_risk_min_bad_rate)
    ].copy()

    high_risk = high_risk.sort_values(["bad_rate", "total_hits"], ascending=[False, False]).head(kw_cfg.topn_keywords)

    return {
        "keyword_stats": stats.reset_index(drop=True),
        "high_risk_keywords": high_risk.reset_index(drop=True),
    }


def slice_analysis(df_valid: pd.DataFrame, cfg: AppConfig) -> Dict[str, pd.DataFrame]:
    """
    分层：
    - length bucket
    - contains english
    - source（若无则说明）
    """
    out: Dict[str, pd.DataFrame] = {}
    df = df_valid.copy()

    df["text_len"] = df["news_raw"].astype(str).apply(lambda x: len(x))
    df["contains_english"] = df["news_raw"].astype(str).str.contains(r"[A-Za-z]", regex=True)

    # length bucket
    def bucket_len(n: int) -> str:
        for ub, label in cfg.slice_cfg.length_buckets:
            if n <= ub:
                return label
        return str(cfg.slice_cfg.length_buckets[-1][1])

    df["len_bucket"] = df["text_len"].apply(bucket_len)

    def agg_table(col: str, name: str) -> pd.DataFrame:
        g = df.groupby(col, dropna=False)
        rows = []
        for val, sub in g:
            count = len(sub)
            err = int((sub["human_check_norm"] == "FALSE").sum())
            rate = round(err / count, 4) if count else 0.0
            rows.append({"slice": str(val), "count": count, "error_count": err, "error_rate": rate})
        t = pd.DataFrame(rows).sort_values(["error_rate", "count"], ascending=[False, False])
        t.insert(0, "slice_type", name)
        return t

    out["length_bucket"] = agg_table("len_bucket", "length_bucket")
    out["contains_english"] = agg_table("contains_english", "contains_english")

    # source
    if "source" in df.columns and df["source"].notna().any():
        out["source"] = agg_table("source", "source")
    else:
        out["source_note"] = pd.DataFrame([{"note": "无 source 字段（或全为空），已跳过来源分层。"}])

    # Top5 最容易错的层（合并挑选）
    combined = []
    for k, t in out.items():
        if k.endswith("_note"):
            continue
        if isinstance(t, pd.DataFrame) and {"slice_type", "slice", "error_rate", "count"}.issubset(set(t.columns)):
            combined.append(t)
    if combined:
        c = pd.concat(combined, ignore_index=True)
        out["top_error_slices"] = c.sort_values(["error_rate", "count"], ascending=[False, False]).head(5).reset_index(drop=True)

    return out


def build_suggestions(
    df_valid: pd.DataFrame,
    kw_tables: Dict[str, pd.DataFrame],
    acc_df: pd.DataFrame,
    slice_tables: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    输出建议清单（>=10 条，尽量基于证据，不空泛）。
    """
    suggestions: List[Dict[str, Any]] = []
    sid = 1

    # 1) 高风险关键词：优先建议
    high_risk = kw_tables.get("high_risk_keywords", pd.DataFrame())
    if not high_risk.empty:
        for _, r in high_risk.head(8).iterrows():
            kw = r["keyword"]
            total = int(r["total_hits"])
            bad = int(r["bad_hits"])
            br = float(r["bad_rate"])
            suggestions.append({
                "suggestion_id": sid,
                "type": "白名单/组合规则/词表收敛",
                "target": f"keyword:{kw}",
                "evidence": f"{kw} total={total} bad={bad} bad_rate={br}",
                "proposal": "对该关键词做：1) 加词边界/大小写/位置约束；2) 结合更强主体词共同命中；3) 视情况加入白名单场景。",
            })
            sid += 1

    # 2) accuracy 最差的 decision/country/rule_hit
    # acc_df: group_type/group_value/count/true/false/accuracy
    for gt in ["decision", "country", "rule_hit"]:
        sub = acc_df.loc[acc_df["group_type"] == gt].copy()
        if sub.empty:
            continue
        worst = sub.sort_values(["accuracy", "count"], ascending=[True, False]).head(2)
        for _, r in worst.iterrows():
            gv = r["group_value"]
            acc = float(r["accuracy"])
            cnt = int(r["count"])
            suggestions.append({
                "suggestion_id": sid,
                "type": "优先级调整/规则拆分",
                "target": f"{gt}:{gv}",
                "evidence": f"{gt}={gv} accuracy={acc} count={cnt}",
                "proposal": "对该分组做规则复盘：1) 检查该类命中关键词是否过宽；2) 引入排除词/白名单；3) 对高歧义词改为人审或要求组合命中。",
            })
            sid += 1

    # 3) slice error top
    top_slices = slice_tables.get("top_error_slices", pd.DataFrame())
    if not top_slices.empty:
        for _, r in top_slices.iterrows():
            st = r["slice_type"]
            sv = r["slice"]
            er = float(r["error_rate"])
            cnt = int(r["count"])
            suggestions.append({
                "suggestion_id": sid,
                "type": "场景策略",
                "target": f"{st}:{sv}",
                "evidence": f"{st}={sv} error_rate={er} count={cnt}",
                "proposal": "对该场景单独策略：例如短文本强制人审比例更高；含英文时增强词边界；来源特定时加入白名单或专用词表。",
            })
            sid += 1

    # 4) 兜底：若建议不足 10 条，补充通用但可执行的建议（带 evidence=overall）
    overall = acc_df.loc[(acc_df["group_type"] == "overall") & (acc_df["group_value"] == "ALL")]
    if not overall.empty:
        oa = float(overall.iloc[0]["accuracy"])
        total = int(overall.iloc[0]["count"])
    else:
        oa, total = np.nan, len(df_valid)

    while len(suggestions) < 10:
        suggestions.append({
            "suggestion_id": sid,
            "type": "词表增删/规则组合",
            "target": "global",
            "evidence": f"overall_accuracy={oa} total_valid={total}",
            "proposal": "补充：1) 对高频但低贡献关键词做降级；2) 引入‘强主体词’优先；3) 对多国共现新增规则：强制人审或输出多国并降级决策。",
        })
        sid += 1

    return pd.DataFrame(suggestions)


def build_overview(df_valid: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    cdist = country_distribution(df_valid)
    ddist = decision_distribution(df_valid)
    unrec = unrecognized_ratio(df_valid)
    ct_count, ct_rowpct = country_decision_crosstab(df_valid)

    return {
        "country_distribution": cdist,
        "decision_distribution": ddist,
        "unrecognized_ratio": unrec,
        "country_x_decision_count": ct_count,
        "country_x_decision_rowpct": ct_rowpct,
    }


def evaluate(cfg: AppConfig = CONFIG) -> EvalOutputs:
    raw_df, used_sheet = read_input_excel(cfg)
    mapped = apply_column_mapping(raw_df, cfg)

    cleaned_raw_df, issues_to_style, quality_issues_df = compute_quality_flags(mapped, cfg)

    # 有效行：bad_row=False
    df_valid = cleaned_raw_df.loc[cleaned_raw_df["bad_row"] == False].copy()
    skipped = int((cleaned_raw_df["bad_row"] == True).sum())
    total = len(cleaned_raw_df)
    logger.info("总行数=%d | 有效行=%d | 跳过坏行=%d", total, len(df_valid), skipped)

    overview_tables = build_overview(df_valid)

    acc_df = accuracy_tables(df_valid)

    confusion_payload = confusion_or_fallback(df_valid)

    top_wrong_df = top_wrong_cases(df_valid, topn=10)

    kw_tables = keyword_risk(df_valid, cfg)

    slice_tables = slice_analysis(df_valid, cfg)

    suggestions_df = build_suggestions(df_valid, kw_tables, acc_df, slice_tables)

    # quality_issues sheet 需要额外汇总“跳过数量”
    quality_summary = pd.DataFrame([{
        "total_rows": total,
        "valid_rows": len(df_valid),
        "skipped_bad_rows": skipped,
        "input_sheet_used": used_sheet,
        "input_file": cfg.paths.input_excel,
    }])

    meta = {
        "total_rows": total,
        "valid_rows": len(df_valid),
        "skipped_bad_rows": skipped,
        "input_sheet_used": used_sheet,
        "input_file": cfg.paths.input_excel,
        "output_file": cfg.paths.output_excel,
    }

    # quality_issues_df 增强：加总览
    quality_issues_full = {
        "summary": quality_summary,
        "reason_distribution": quality_issues_df,
    }

    return EvalOutputs(
        cleaned_raw_df=cleaned_raw_df,
        quality_issues_df=pd.DataFrame(),  # 旧字段占位（我们用 dict 输出到 export）
        overview_tables=overview_tables,
        accuracy_breakdown_df=acc_df,
        confusion_payload=confusion_payload,
        top_wrong_cases_df=top_wrong_df,
        keyword_risk_tables=kw_tables,
        slice_tables=slice_tables,
        suggestions_df=suggestions_df,
        issues_to_style=issues_to_style,
        meta={"quality_issues": quality_issues_full, **meta},
    )
