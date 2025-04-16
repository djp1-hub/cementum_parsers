import os
import datetime
import json
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()


class VersionTracker:
    def __init__(self, dbname, user="postgres", password="password", host="localhost", port=5432):
        user =  os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        host = os.getenv("POSTGRES_HOST")
        port = int(os.getenv("POSTGRES_PORT", 5432))
        try:
            self.engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")
        except:
            print("fail")
        self.ensure_table_exists()

    def ensure_table_exists(self):
        with self.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS file_versions (
                    id SERIAL PRIMARY KEY,
                    file_path TEXT NOT NULL,
                    modified_at TIMESTAMP NOT NULL,
                    config JSONB,
                    table_name TEXT,
                    success BOOLEAN DEFAULT FALSE
                );
            """))

    def get_latest_version(self, file_path: str):
        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                    SELECT modified_at FROM file_versions
                    WHERE file_path = :path AND success
                    ORDER BY update_datetime DESC
                    LIMIT 1
                """),
                {"path": file_path}
            ).fetchone()
        return result.modified_at if result else None

    def is_newer(self, file_path: str) -> bool:
        mtime = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
        last = self.get_latest_version(file_path)
        return last is None or mtime > last

    def insert_file_version(self, file_path: str, config: dict) -> int:
        mtime = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
        config_json = json.dumps(config)
        table_name = config.get("TableName")
        dbname = config.get("dbname")

        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO file_versions (file_path, modified_at, config, table_name, success)
                    VALUES (:path, :mtime, :config, :table_name, FALSE)
                    RETURNING id;
                """),
                {"path": file_path, "mtime": mtime, "config": config_json, "table_name": dbname + '.' + table_name}
            ).fetchone()

        return result.id if result else None

    def mark_success(self, file_version_id: int):
        with self.engine.begin() as conn:
            conn.execute(
                text("UPDATE file_versions SET success = TRUE WHERE id = :id"),
                {"id": file_version_id}
            )
