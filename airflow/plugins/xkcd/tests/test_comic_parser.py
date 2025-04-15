import sys
from pathlib import Path
from datetime import datetime

# Add plugins directory to Python path
plugins_dir = str(Path(__file__).parents[2])
if plugins_dir not in sys.path:
    sys.path.append(plugins_dir)

from xkcd.utils.comic_parser import ComicParser


def test_comic_parser():
    # Test data
    sample_comic_data = {
        "num": 1,
        "title": "Test Comic",
        "alt": "Test Alt Text",
        "img": "https://example.com/image.png",
        "year": "2023",
        "month": "1",
        "day": "1",
        "transcript": "Test transcript",
        "extra_field": "extra value"
    }

    # Test parsing
    parsed_data = ComicParser.parse_comic_data(sample_comic_data)
    assert parsed_data is not None
    print("Parsed comic data:", parsed_data)

    # Test database record conversion
    db_record = ComicParser.to_db_record(parsed_data)
    print("Database record:", db_record)

    # Test SQL query generation
    insert_query = ComicParser.generate_insert_query()
    print("Insert query:", insert_query)

    # Test with missing fields
    invalid_data = {
        "num": 1,
        "title": "Test Comic"
    }
    assert ComicParser.parse_comic_data(invalid_data) is None

    print("All tests passed!")


if __name__ == "__main__":
    test_comic_parser()