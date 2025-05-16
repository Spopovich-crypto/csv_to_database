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

def main():
    print("Hello from csv-to-database!")


if __name__ == "__main__":
    main()
