# 机审规则结果评估与复盘（Excel 输入 → 多 sheet 报告 + 颜色标记）

## 项目简介
本项目模拟机审策略业务场景，以新闻领域为例，设计审核规则后观察机器审核效果，如规则准确性、关键词准确性等，根据效果反馈收严或放宽审核规则，使其准确性最大化

## 功能概览
输入：audit_samples.xlsx（默认 sheet=raw）  
输出：audit_report.xlsx（多 sheet 报告 + cleaned_raw 颜色标记）

核心能力：
- 数据质量检查：缺失/空字符串/非法值/重复 id 等 → 标记 bad_row + bad_reason，并在 cleaned_raw 里对问题单元格上色
- 统计只基于有效行（bad_row=False），并输出“跳过多少行 + 原因分布”
- 覆盖率/分布：country、decision、未识别比例、country×decision 交叉表（count & row%）
- 效果评估：基于 human_check(TRUE/FALSE) 的 overall accuracy + 按 decision/country/rule_hit 分组 accuracy
- 可选：若存在 human_decision 列（删/人审/通过），自动输出混淆矩阵与误删率/漏删率
- Top wrong cases：按 decision 分组各取 Top10，含 why_wrong（保守模板推断）
- 关键词风险：拆分 triggered_keywords，统计 total_hits/bad_hits/bad_rate，高风险词筛选
- 分层分析：长度桶/是否含英文/source 分层样本数与错误率
- 迭代建议：输出 suggestions 表（>=10 条，证据引用统计结果）

## 安装
建议 Python 3.10+。

```bash
pip install -r requirements.txt
