from typing import List, Dict, Any, Optional
import logging
from contextlib import contextmanager
from airflow.providers.postgres.hooks.postgres import PostgresHook
from xkcd.config import (
    DEFAULT_POSTGRES_CONN_ID,
    DEFAULT_SCHEMA,
    DEFAULT_TABLE,
    DEFAULT_BATCH_SIZE,
    DEFAULT_LOG_LEVEL
)
from xkcd.utils.comic_parser import ComicData, ComicParser

logger = logging.getLogger(__name__)
logger.setLevel(DEFAULT_LOG_LEVEL)


class XKCDPostgresHook(PostgresHook):
    """
    Extended PostgresHook for XKCD comic data operations
    """

    def __init__(
            self,
            postgres_conn_id: str = DEFAULT_POSTGRES_CONN_ID,
            db_schema: str = DEFAULT_SCHEMA,
            table: str = DEFAULT_TABLE
    ) -> None:
        """
        Initialize XKCDPostgresHook

        Args:
            postgres_conn_id: Airflow connection ID for Postgres
            db_schema: Database schema name
            table: Table name
        """
        super().__init__(postgres_conn_id=postgres_conn_id)
        self.db_schema = db_schema
        self.table = table
        self.parser = ComicParser()

    @contextmanager
    def get_cursor(self):
        """
        Context manager for database cursor

        Yields:
            Database cursor
        """
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()

    def get_max_comic_num(self) -> int:
        """
        Get the highest comic number from the database

        Returns:
            Maximum comic number or 0 if table is empty
        """
        query = f"""
            SELECT COALESCE(MAX(num), 0)
            FROM {self.db_schema}.{self.table}
        """

        try:
            with self.get_cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                max_num = result[0] if result else 0
                logger.info(f"Latest comic from database: {max_num}")
                return max_num
        except Exception as e:
            logger.error(f"Failed to get max comic number: {str(e)}")
            raise

    def insert_single_comic(self, comic_data: ComicData) -> bool:
        """
        Insert a single comic into database

        Args:
            comic_data: ComicData object to insert
        Returns:
            True if insertion successful, False otherwise
        """
        insert_query = self.parser.generate_insert_query()
        values = tuple(self.parser.to_db_record(comic_data).values())

        try:
            with self.get_cursor() as cursor:
                cursor.execute(insert_query, values)
                logger.info(f"Successfully inserted comic #{comic_data.num}")
                return True
        except Exception as e:
            logger.error(f"Failed to insert comic #{comic_data.num}: {str(e)}")
            return False

# todo: Add batch insert method