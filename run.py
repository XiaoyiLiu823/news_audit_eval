from __future__ import annotations

import logging
import sys

from config import CONFIG
from eval import evaluate
from export_excel import export_report


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def main() -> int:
    setup_logging()

    try:
        outputs = evaluate(CONFIG)
        export_report(outputs, CONFIG)

        print("\n完成。")
        print(f"- 输入: {CONFIG.paths.input_excel}")
        print(f"- 输出: {CONFIG.paths.output_excel}")
        print(f"- 总行数: {outputs.meta.get('total_rows')}, 有效行: {outputs.meta.get('valid_rows')}, 跳过坏行: {outputs.meta.get('skipped_bad_rows')}")
        return 0

    except Exception as e:
        print(f"[错误] {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
