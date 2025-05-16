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


def read_pi_file(file_path: Path, encoding="utf-8") -> list[dict]:
    import polars as pl

    with open(file_path, encoding=encoding) as f:
        lines = [next(f) for _ in range(3)]

        param_names = lines[1].strip().split(",")
        param_names[0] = "Datetime"

        lf = pl.scan_csv(
            file_path,
            has_header=False,
            new_columns=param_names,
            skip_rows=3,
            infer_schema_length=None,
            try_parse_dates=True,
        )
        
        # 全部の列がnullでない行をフィルタリング
        lf = lf.filter(pl.all_horizontal(pl.all().is_not_null()))
        
        # パーティショニング用パーツ抽出
        lf = lf.with_columns(
            [
                pl.col("Datetime").dt.year().alias("year"),
                pl.col("Datetime").dt.month().alias("month")
            ]
        )
        
    return lf


def main():
    print("Hello from csv-to-database!")


if __name__ == "__main__":
    main()
