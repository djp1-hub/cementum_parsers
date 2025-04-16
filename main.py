import os
import sys
import pandas as pd
from parsers import PARSERS
from parsers.postgres_writer import PostgresWriter
from parsers.version_tracker import VersionTracker
import json
import ast
import fnmatch


DESCRIPTION_FILENAME = "description.xlsx"


def find_valid_folders(root_dir):
    """
    Рекурсивно находит все поддиректории, содержащие description.xlsx
    """
    valid_folders = []
    for dirpath, dirnames, filenames in os.walk(root_dir):
        if DESCRIPTION_FILENAME in filenames:
            valid_folders.append(dirpath)
    return valid_folders


def get_description_config(folder_path: str) -> dict:
    description_path = os.path.join(folder_path, DESCRIPTION_FILENAME)
    if not os.path.isfile(description_path):
        raise FileNotFoundError(f"Файл {DESCRIPTION_FILENAME} не найден в {folder_path}")

    df = pd.read_excel(description_path)

    if "key" not in df.columns or "value" not in df.columns:
        raise ValueError("description.xlsx должен содержать колонки 'key' и 'value'")

    config = {}
    for _, row in df.dropna(subset=["key", "value"]).iterrows():
        key = str(row["key"]).strip()
        val = str(row["value"]).strip()

        # Попробуем превратить в list/dict, если нужно
        if key.lower() == "columnlist":
            try:
                config[key] = ast.literal_eval(val)  # безопасный eval
            except Exception:
                config[key] = val  # если не распарсилось — как строка
        elif key.lower() == "additionalparameters":
            try:
                config[key] = json.loads(val)
            except json.JSONDecodeError:
                config[key] = val
        else:
            config[key] = val

    # Проверка на обязательные параметры
    if "ParserInterface" not in config and "parser" not in config:
        raise ValueError("В description.xlsx отсутствует параметр 'parser' или 'ParserInterface'")

    return config



def process_folder(folder_path: str):
    print(f"\n📁 Обработка папки: {folder_path}")

    try:
        config = get_description_config(folder_path)
    except Exception as e:
        print(f"Ошибка при чтении description.xlsx: {e}")
        return

    parser_name = config.get("parser")
    sheet_name = config.get("sheetname")
    tablename = config.get("tablename")
    dbname = config.get("dbname")

    parser_class = PARSERS.get(parser_name)
    if not parser_class:
        print(f"Парсер '{parser_name}' не найден. Доступные: {list(PARSERS.keys())}")
        return

    try:
        writer = PostgresWriter(config)
        version_tracker = VersionTracker(dbname=dbname)
    except Exception as e:
        print(f"Ошибка подключения к БД {dbname}: {e}")
        return

    MAX_FILES = 100
    processed = 0
    file_mask = config.get("FileName")  # например: "data_*.xlsx"
    all_files = os.listdir(folder_path)

    if file_mask:
        files_to_process = fnmatch.filter(all_files, file_mask)
    else:
        files_to_process = all_files

    for fname in files_to_process:
        print(fname)
        if processed >= MAX_FILES:
            print(f"⚠️ Достигнут лимит в {MAX_FILES} файлов. Остановлено.")
            break

        if fname == DESCRIPTION_FILENAME:
            continue

        fpath = os.path.join(folder_path, fname)

        if not version_tracker.is_newer(fpath):
            print(f"[SKIP] {fname} не изменён с последней обработки.")
            continue

        try:
            parser = parser_class(fpath, config)
            result = parser.parse()

            file_version_id = version_tracker.insert_file_version(fpath, config)

            # Добавляем к DataFrame
            if isinstance(result, dict):
                for _, df in result.items():
                    df["file_version_id"] = file_version_id
                    writer.write(df)
            else:
                result["file_version_id"] = file_version_id
                writer.write(result)

            version_tracker.mark_success(file_version_id)
            print(f"[OK] Загружен файл: {fname}")
            processed += 1

        except Exception as e:
            print(f"[ERROR] Ошибка при обработке файла {fname}: {e}")



def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py <folder_path_or_root_dir>")
        sys.exit(1)

    root_path = sys.argv[1]

    if not os.path.exists(root_path):
        print(f"Путь {root_path} не существует")
        sys.exit(1)

    if os.path.isdir(root_path):
        folders_to_process = find_valid_folders(root_path)
        if not folders_to_process:
            print(f"Не найдено ни одной папки с {DESCRIPTION_FILENAME} в {root_path}")
            sys.exit(0)
    else:
        folders_to_process = [root_path]

    for folder in folders_to_process:
        process_folder(folder)


if __name__ == "__main__":
    main()
