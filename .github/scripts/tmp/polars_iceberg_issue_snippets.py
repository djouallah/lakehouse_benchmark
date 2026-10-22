"""Run every python block of the drafted Polars Iceberg issues; print what each one does."""

import json
import pathlib

blocks = json.loads(pathlib.Path(__file__).with_suffix(".json").read_text())
for title, code in blocks:
    print(f"\n=== {title}")
    try:
        exec(compile(code, title, "exec"), {})
        print("RAN WITHOUT ERROR")
    except Exception as e:
        print(f"{type(e).__name__}: {str(e)[:400]}")
