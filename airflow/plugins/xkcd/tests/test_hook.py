import sys
import logging
from pathlib import Path

# Add plugins directory to Python path
plugins_dir = str(Path(__file__).parents[2])
if plugins_dir not in sys.path:
    sys.path.append(plugins_dir)

from xkcd.hooks.xkcd_hook import XKCDHook


def test_hook():
    logging.basicConfig(level=logging.INFO)

    hook = XKCDHook()

    # Test getting latest comic
    print("\nTesting get_latest_comic_num:")
    latest_num = hook.get_latest_comic_num()
    print(f"Latest comic number: {latest_num}")

    # Test getting specific comic
    print("\nTesting get_comic_by_num:")
    comic = hook.get_comic_by_num(1)
    print(f"First comic: {comic}")

    # Test retry mechanism
    print("\nTesting invalid comic number:")
    invalid_comic = hook.get_comic_by_num(999999)
    print(f"Invalid comic result: {invalid_comic}")


if __name__ == "__main__":
    test_hook()