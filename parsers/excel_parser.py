import pandas as pd
from .base import BaseParser

class ExcelParser(BaseParser):
    name = "excel"

    def __init__(self, file_path: str, config: dict):
        self.file_path = file_path
        self.config = config

    def parse(self):
        sheet_name = self.config.get("SheetName")
        usecols = self.config.get("ColumnList")
        additional = self.config.get("AdditionalParameters", {})
        skiprows = self.config.get("skiprows", 0)

        print(f"[ExcelParser] Файл: {self.file_path}, Лист: {sheet_name or '(по умолчанию)'}")
        print(f"usecols: {usecols}")

        df = pd.read_excel(
            self.file_path,
            sheet_name=sheet_name,
            usecols=usecols,
            skiprows=skiprows,
            #**additional  # можно передать skiprows, nrows и т.д.
        )

        # Если несколько листов — выводим каждый
        if isinstance(df, dict):
            for sheet, d in df.items():
                print(f"→ Лист: {sheet}")
                print(d.head())
        else:
            print(df.head())

        return df
