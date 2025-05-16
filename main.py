import polars as pl
from pathlib import Path


def search_csv_file(target_path: Path, file_name_pattern: list[str]) -> list[Path]:
    target_csv_files = []

    csv_files = target_path.rglob("*.csv")

    if not csv_files:
        print(f"No CSV files found in {target_path}")
        return target_csv_files
    if len(file_name_pattern) == 0:
        target_csv_files = list(csv_files)
        return target_csv_files
    else:
        for cscv_file in csv_files:
            if any(pat in cscv_file.name for pat in file_name_pattern):
                target_csv_files.append(cscv_file)
        return target_csv_files



def read_pi_file(file_path: Path, encoding="utf-8"):
    # ── 1. ヘッダー読み込み（3行） ─────────────────────
    with open(file_path, encoding=encoding) as f:
        header = [next(f) for _ in range(3)]
    
    # 各行をカンマ区切りで分割 → [[ID1, ID2, ...], [name1, name2, ...], [unit1, unit2, ...]]
    header = [row.strip().split(",") for row in header]

    # 先頭列名を"Datetime"に変更
    param_ids = header[0]
    param_ids[0] = "Datetime"  # 元々の1列目は日時情報

    # ── 2. センサデータ本体のLazyFrame化 ───────────────
    lf = pl.scan_csv(
        file_path,
        has_header=False,
        new_columns=param_ids,
        skip_rows=3,
        try_parse_dates=True,
        infer_schema_length=None,
    )

    # null行の除去
    lf = lf.filter(pl.all_horizontal(pl.all().is_not_null()))

    # パーティショニング用列追加
    lf = lf.with_columns([
        pl.col("Datetime").dt.year().alias("year"),
        pl.col("Datetime").dt.month().alias("month")
    ])

    # ── 3. ヘッダー情報の縦持ちLazyFrame構築 ──────────
    # 1列目は "Datetime" なので除外
    ids, names, units = header[0][1:], header[1][1:], header[2][1:]
    header_records = list(zip(ids, names, units))

    header_lf = pl.LazyFrame(
        header_records,
        schema=["param_id", "param_name", "unit"]
    )

    return lf, header_lf
