import json
from datetime import datetime
from typing import Dict, Any, Optional
import logging
from dataclasses import dataclass
from ..config import DEFAULT_SCHEMA, DEFAULT_TABLE, DEFAULT_LOG_LEVEL

logger = logging.getLogger(__name__)
logger.setLevel(DEFAULT_LOG_LEVEL)


@dataclass
class ComicData:
    """Data class for storing parsed comic information"""
    num: int
    title: str
    alt_text: str
    img_url: str
    published_date: datetime
    transcript: Optional[str]
    raw_data: Dict[str, Any]


class ComicParser:
    """Parser for XKCD comic data"""

    @staticmethod
    def parse_date(year: str, month: str, day: str) -> datetime:
        """
        Parse date components into datetime object

        Args:
            year: Year string
            month: Month string
            day: Day string
        Returns:
            datetime object
        """
        try:
            return datetime(int(year), int(month), int(day))
        except ValueError as e:
            logger.error(f"Failed to parse date: {year}-{month}-{day}, error: {str(e)}")
            raise

    @classmethod
    def parse_comic_data(cls, raw_data: Dict[str, Any]) -> Optional[ComicData]:
        """
        Parse raw API response into structured comic data

        Args:
            raw_data: Raw API response dictionary
        Returns:
            ComicData object if parsing successful, None otherwise
        """
        try:
            # Validate required fields
            required_fields = ['num', 'title', 'alt', 'img', 'year', 'month', 'day']
            missing_fields = [field for field in required_fields if field not in raw_data]

            if missing_fields:
                logger.error(f"Missing required fields: {missing_fields}")
                return None

            # Parse date
            published_date = cls.parse_date(
                raw_data['year'],
                raw_data['month'],
                raw_data['day']
            )

            return ComicData(
                num=int(raw_data['num']),
                title=raw_data['title'],
                alt_text=raw_data['alt'],
                img_url=raw_data['img'],
                published_date=published_date,
                transcript=raw_data.get('transcript'),  # Optional field
                raw_data=raw_data
            )

        except Exception as e:
            logger.error(f"Failed to parse comic data: {str(e)}")
            logger.error(f"Raw data: {raw_data}")
            return None

    @staticmethod
    def to_db_record(comic_data: ComicData) -> Dict[str, Any]:
        """
        Convert ComicData to database record format

        Args:
            comic_data: ComicData object
        Returns:
            Dictionary formatted for database insertion
        """
        return {
            'num': comic_data.num,
            'title': comic_data.title,
            'alt_text': comic_data.alt_text,
            'img_url': comic_data.img_url,
            'published_date': comic_data.published_date,
            'transcript': comic_data.transcript,
            'raw_data': json.dumps(comic_data.raw_data),
            'loaded_at': datetime.now()
        }

    @staticmethod
    def generate_insert_query() -> str:
        """
        Generate SQL insert query for comic data

        Returns:
            SQL insert query string
        """
        columns = [
            'num',
            'title',
            'alt_text',
            'img_url',
            'published_date',
            'transcript',
            'raw_data',
            'loaded_at'
        ]

        return f"""
            INSERT INTO {DEFAULT_SCHEMA}.{DEFAULT_TABLE}
            ({', '.join(columns)})
            VALUES
            ({', '.join(['%s' for _ in columns])})
            ON CONFLICT (num) DO UPDATE SET
            {', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'num'])}
        """