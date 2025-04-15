import sys
from pathlib import Path

# Add plugins directory to Python path
plugins_dir = str(Path(__file__).parents[2])
if plugins_dir not in sys.path:
    sys.path.append(plugins_dir)

from xkcd.hooks.xkcd_api_hook import XKCDApiHook

def test_api_hook():
    hook = XKCDApiHook()

    # Test getting latest comic
    latest_num = hook.get_latest_comic_num()
    print(f"Latest comic number: {latest_num}")

    # Test getting specific comic
    comic = hook.get_comic_by_num(1)
    print(f"Comic #1: {comic}")

    # Test retry mechanism
    print("\nTesting invalid comic number:")
    invalid_comic = hook.get_comic_by_num(999999)
    print(f"Invalid comic result: {invalid_comic}")


if __name__ == "__main__":
    test_api_hook()