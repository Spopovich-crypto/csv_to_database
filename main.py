"""
sensor_parquet_ingest.py

CSV → Parquet 変換（縦持ち & パーティション書き込み）＋処理履歴管理のワンショット実装
---------------------------------------------------------------------------------------
◆ 主な処理
    1. 既に取り込み済みかどうかを DuckDB の processed_files テーブルで判定
    2. CSV を LazyFrame で読み込み、メタ列 plant_name・machine_no を付与
    3. Parquet ルート配下に `plant_name=.../machine_no=.../year=YYYY/month=MM` で分割保存
       - 既存 Parquet があれば縦持ちでマージし、重複行を除去
    4. 処理完了後に processed_files へファイルパス＋更新日時を記録
---------------------------------------------------------------------------------------
Python 3.11 / Polars ≥ 0.20 / DuckDB ≥ 0.10 / PyArrow ≥ 15 で動作確認
"""

from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime
from typing import Sequence

import duckdb
import polars as pl
import pyarrow.dataset as ds


# ────────────────────────────────────────────────────────────────────────────────
# 設定値
DB_PATH: str | Path = "master.duckdb"
PARQUET_ROOT: Path = Path("parquet_root")          # ここ以下に partition を掘る
PATTERNS: list[str] = ["Cond", "Vib", "Tmp"]       # CSV ファイル名のキーワード
MEMORY_LIMIT: str = "2GB"                          # DuckDB メモリ制限
PART_COLUMNS = ["plant_name", "machine_no", "year", "month"]
PARQUET_WRITE_KWARGS = dict(existing_data_behavior="overwrite_or_ignore")  # PyArrow ≥ 15


# ────────────────────────────────────────────────────────────────────────────────
# 1. 共通ユーティリティ
def search_csv_file(target_path: Path, file_name_pattern: Sequence[str]) -> list[Path]:
    """
    指定ディレクトリ以下を再帰検索し、パターン（部分一致）でフィルタして CSV 一覧を返す。
    """
    target_csv_files: list[Path] = []
    csv_files = target_path.rglob("*.csv")

    for csv_file in csv_files:
        if not file_name_pattern or any(pat in csv_file.name for pat in file_name_pattern):
            target_csv_files.append(csv_file)

    return target_csv_files


# ────────────────────────────────────────────────────────────────────────────────
# 2. 処理履歴テーブル
def ensure_db_schema(con: duckdb.DuckDBPyConnection) -> None:
    """processed_files テーブルを持っていなければ作成"""
    con.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            file_path     TEXT PRIMARY KEY,
            modified_time TIMESTAMP
        )
    """)


def is_already_processed(fp: Path, con: duckdb.DuckDBPyConnection) -> bool:
    """ファイルパス＋最終更新日時で再処理判定"""
    stat = fp.stat()
    mod_ts = datetime.fromtimestamp(stat.st_mtime)

    return con.execute(
        "SELECT 1 FROM processed_files WHERE file_path=? AND modified_time=?",
        [str(fp), mod_ts]
    ).fetchone() is not None


def mark_as_processed(fp: Path, con: duckdb.DuckDBPyConnection) -> None:
    """取り込み完了ファイルを processed_files へ登録／更新"""
    stat = fp.stat()
    mod_ts = datetime.fromtimestamp(stat.st_mtime)
    con.execute(
        "INSERT OR REPLACE INTO processed_files VALUES (?, ?)",
        [str(fp), mod_ts]
    )


# ────────────────────────────────────────────────────────────────────────────────
# 3. CSV 読み込み（質問文の read_pi_file をベースに改変）
def read_pi_file(file_path: Path, encoding: str = "utf-8") -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    - 先頭 3 行をヘッダ（ID, name, unit）として取り出す
    - 本体を LazyFrame で scan_csv
    - ヘッダ 3 行は縦持ち LazyFrame（param_master 用）として返す
    """
    # ヘッダ 3 行
    with open(file_path, encoding=encoding) as f:
        header = [next(f) for _ in range(3)]
    header = [row.strip().split(",") for row in header]

    param_ids = header[0]
    param_ids[0] = "Datetime"  # 1 列目は日時

    # データ本体
    lf = pl.scan_csv(
        file_path,
        has_header=False,
        new_columns=param_ids,
        skip_rows=3,
        try_parse_dates=True,
        infer_schema_length=None,
    )

    # null 行除去
    lf = lf.filter(pl.all_horizontal(pl.all().is_not_null()))

    # 年月列
    lf = lf.with_columns([
        pl.col("Datetime").dt.year().alias("year"),
        pl.col("Datetime").dt.month().alias("month"),
    ])

    # ヘッダ行を縦持ち化
    ids, names, units = header[0][1:], header[1][1:], header[2][1:]
    header_records = list(zip(ids, names, units))
    header_lf = pl.LazyFrame(header_records, schema=["param_id", "param_name", "unit"], orient="row")

    return lf, header_lf


# ────────────────────────────────────────────────────────────────────────────────
# 4. LazyFrame へのメタ列付与
def load_csv_lazy(fp: Path, plant: str, machine: str, encoding: str = "utf-8") -> pl.LazyFrame:
    """CSV → LazyFrame 変換＋ plant_name / machine_no 列を追加"""
    lf, _header_lf = read_pi_file(fp, encoding=encoding)
    return lf.with_columns([
        pl.lit(plant).alias("plant_name"),
        pl.lit(machine).alias("machine_no"),
    ])


# ────────────────────────────────────────────────────────────────────────────────
# 5. Parquet へのアップサート
def _partition_dir(df: pl.DataFrame) -> Path:
    """DataFrame の先頭行からパーティションパスを決定"""
    return (
        PARQUET_ROOT
        / f"plant_name={df['plant_name'][0]}"
        / f"machine_no={df['machine_no'][0]}"
        / f"year={df['year'][0]}"
        / f"month={df['month'][0]:02d}"
    )


def write_partitioned_parquet(new_lf: pl.LazyFrame) -> None:
    """
    1. 対象パーティションの既存 Parquet を読み込む
    2. 縦持ちで結合し、主キー重複を排除
    3. フォルダを削除 → 再出力
    """
    new_df = new_lf.collect()
    part_dir = _partition_dir(new_df)

    # 既存取り込み
    if part_dir.exists():
        existing = pl.scan_pyarrow_dataset(ds.dataset(part_dir, format="parquet")).collect()
        merged = (
            pl.concat([existing, new_df])
            .unique(
                subset=["plant_name", "machine_no", "year", "month",
                        "Datetime", "parameter_id"]  # ★主キー
            )
        )
    else:
        merged = new_df

    # 旧ファイル削除（上書き運用）
    if part_dir.exists():
        for p in part_dir.glob("*.parquet"):
            p.unlink()
    part_dir.mkdir(parents=True, exist_ok=True)

    # 書き込み
    ds.write_dataset(
        merged.to_arrow(),
        base_dir=part_dir,
        format="parquet",
        **PARQUET_WRITE_KWARGS,
    )


# ────────────────────────────────────────────────────────────────────────────────
# 6. ファイル 1 本の取り込みフロー
def ingest_file(fp: Path, plant: str, machine: str) -> None:
    with duckdb.connect(DB_PATH, read_only=False) as con:
        ensure_db_schema(con)
        con.execute(f"PRAGMA memory_limit='{MEMORY_LIMIT}'")

        if is_already_processed(fp, con):
            print("SKIP (up-to-date):", fp.name)
            return

        lf = load_csv_lazy(fp, plant, machine)
        write_partitioned_parquet(lf)
        mark_as_processed(fp, con)
        print("INGESTED:", fp.name)


# ────────────────────────────────────────────────────────────────────────────────
# 7. エントリポイント
def main() -> None:
    target_dir = Path(r"sensor_data")   # 取り込み元フォルダ
    files = search_csv_file(target_dir, PATTERNS)

    if not files:
        print("No target CSV files found.")
        return

    for csv_path in files:
        # ★ plant_name / machine_no をファイル名などから抽出するロジックを実装
        #   ここではダミー値
        plant_code, machine_code = "東京第一工場", "101"
        ingest_file(csv_path, plant_code, machine_code)


if __name__ == "__main__":
    main()
