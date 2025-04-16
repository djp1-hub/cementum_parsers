import pandas as pd
from io import StringIO
from sqlalchemy.engine import create_engine
import re
from dotenv import load_dotenv
import os

load_dotenv()

def transliterate_column_name(name: str) -> str:
    translit_map = {
        'А': 'A', 'Б': 'V', 'В': 'V', 'Г': 'G', 'Д': 'D',
        'Е': 'E', 'Ё': 'E', 'Ж': 'ZH', 'З': 'Z', 'И': 'I',
        'Й': 'I', 'К': 'K', 'Л': 'L', 'М': 'M', 'Н': 'N',
        'О': 'O', 'П': 'R', 'Р': 'R', 'С': 'S', 'Т': 'T',
        'У': 'U', 'Ф': 'F', 'Х': 'KH', 'Ц': 'TC', 'Ч': 'CH',
        'Ш': 'SH', 'Щ': 'SHCH', 'Ы': 'Y', 'Э': 'E', 'Ю': 'IU', 'Я': 'IA'
    }
    translit_map.update({k.lower(): v.lower() for k, v in translit_map.items()})
    return ''.join(translit_map.get(ch, ch) for ch in name)

def normalize_column_name(name: str) -> str:
    # транслитерация + замена пробелов + удаление спецсимволов
    name = transliterate_column_name(name)
    name = name.strip().replace(' ', '_')
    name = re.sub(r'[^\w]', '', name)  # только буквы, цифры, _
    return name.lower()

class PostgresWriter:
    def __init__(self, config: dict):
        self.config = config

        self.dbname = config.get("dbname")
        self.table_name = config.get("TableName")
        self.schema = config.get("schema", "excel")  # по умолчанию excel

        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        host = os.getenv("POSTGRES_HOST")
        port = int(os.getenv("POSTGRES_PORT", 5432))
        self.engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{self.dbname}"
        )

    def write(self, df: pd.DataFrame):
        if df.empty:
            print("[PostgresWriter] DataFrame пустой — загрузка пропущена.")
            return

        print(f"[PostgresWriter] Загружаем данные в {self.dbname}.{self.schema}.{self.table_name} ...")

        # Транслитерируем названия столбцов → новые названия для БД
        original_columns = df.columns
        column_map = {col: normalize_column_name(col) for col in original_columns}
        df = df.rename(columns=column_map)

        try:
            conn = self.engine.raw_connection()
            cursor = conn.cursor()

            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            full_table_name = f"{self.schema}.{self.table_name}"
            column_list = ', '.join(f'"{col}"' for col in df.columns)

            cursor.copy_expert(
                f"COPY {full_table_name} ({column_list}) FROM STDIN WITH CSV",
                buffer
            )

            conn.commit()
            cursor.close()
            conn.close()

            print("[PostgresWriter] Загрузка завершена через COPY.")

        except Exception as e:
            print(f"[PostgresWriter] Ошибка COPY: {e}")
            print("[PostgresWriter] Пробуем fallback на to_sql(method='multi')...")
            df.to_sql(
                name=self.table_name,
                con=self.engine,
                if_exists="append",
                index=False,
                schema=self.schema,
                method='multi'
            )
            print("[PostgresWriter] Загрузка завершена через INSERT.")
