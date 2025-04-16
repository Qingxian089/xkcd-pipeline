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


    def insert_single_comic(self, comic_data: Dict[str, Any]) -> bool:
        """
        Insert a single comic into database

        Args:
            comic_data: ComicData object to insert
        Returns:
            True if insertion successful, False otherwise
        """
        insert_query = self.parser.generate_insert_query()
        values = tuple(comic_data.values())

        try:
            with self.get_cursor() as cursor:
                cursor.execute(insert_query, values)
                logger.info(f"Successfully inserted comic #{comic_data.get('num')}")
                return True
        except Exception as e:
            logger.error(f"Failed to insert comic #{comic_data.get('num')}: {str(e)}")
            return False

    def insert_batch_comics(self, comics_data: List[Dict[str, Any]]) -> bool:
        """
        Insert multiple comics in a single batch operation.

        Args:
            comics_data: List of ComicData objects to insert
        Returns:
            Dictionary mapping comic numbers to insertion success status
        """
        if not comics_data:
            logger.warning("Empty batch provided, no action taken")
            return False

        insert_query = self.parser.generate_insert_query()
        values = [tuple(comic.values()) for comic in comics_data]

        try:
            with self.get_cursor() as cursor:
                # Use executemany for batch insertion
                cursor.executemany(insert_query, values)
                # If we get here, all inserts were successful
                comic_nums = [comic.get('num') for comic in comics_data]
                logger.info(
                    f"Successfully batch inserted {len(comics_data)} comics: {min(comic_nums)}-{max(comic_nums)}")
                return True

        except Exception as e:
            logger.error(f"Batch insertion failed: {str(e)}")
            return False