import pandas as pd
from .base import BaseParser
import re
import ast

class UnpivotExcelParser(BaseParser):
    name = "unpivot_excel"

    def __init__(self, file_path: str, config: dict):
        self.file_path = file_path
        self.config = config

    def parse(self):
        # Читаем SheetName и AdditionalParameters
        sheet_name_raw = self.config.get("SheetName")
        additional = self.config.get("AdditionalParameters", {})
        var_name = additional.get('unpivot', {}).get('var_name', 'var')
        value_name = additional.get('unpivot', {}).get('value_name', 'val')

        print(f"[UnpivotExcelParser] Файл: {self.file_path}, SheetName: {sheet_name_raw}")
        #print(additional)

        # Получаем список листов из файла
        all_sheets = pd.ExcelFile(self.file_path).sheet_names

        # Обработка SheetName как строки списка
        try:
            sheet_name_list = ast.literal_eval(sheet_name_raw) if sheet_name_raw else None
        except Exception:
            raise ValueError(f"[UnpivotExcelParser] SheetName должен быть списком: например ['Лист1', 'Лист2'], получено: {sheet_name_raw}")

        # Фильтруем только существующие листы
        if isinstance(sheet_name_list, list):
            valid_sheets = [sheet for sheet in sheet_name_list if sheet in all_sheets]
            if not valid_sheets:
                raise ValueError(f"[UnpivotExcelParser] Ни один из листов {sheet_name_list} не найден в файле. Доступны: {all_sheets}")
        else:
            valid_sheets = sheet_name_list if sheet_name_list in all_sheets else None
            if not valid_sheets:
                raise ValueError(f"[UnpivotExcelParser] Лист {sheet_name_list} не найден в файле. Доступны: {all_sheets}")

        # Загружаем таблицу
        df = pd.read_excel(
            self.file_path,
            sheet_name=valid_sheets
        )

        # Если несколько листов — объединим
        if isinstance(df, dict):
            df = pd.concat(df.values(), ignore_index=True)

        df = df.head(100)  # ограничение для тестов

        # Проверка наличия необходимых колонок
        column_list = self.config.get("ColumnList")
        if not column_list:
            raise ValueError("[UnpivotExcelParser] Не указан параметр ColumnList в config")

        missing = [col for col in column_list if col not in df.columns]
        if missing:
            raise ValueError(f"[UnpivotExcelParser] В таблице отсутствуют колонки: {missing}")

        id_vars = column_list
        value_vars = [
            col for col in df.columns
            if col not in id_vars and re.fullmatch(r"\d{6}", str(col))
        ]

        if not value_vars:
            raise ValueError("[UnpivotExcelParser] Не найдено колонок вида 'YYYYMM' для unpivot.")

        df_unpivoted = df.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name=var_name,
            value_name=value_name
        )

        #print(df_unpivoted.head())
        return df_unpivoted
