import argparse
import json
import re
from pathlib import Path
from typing import Iterable, List, Any

# ╔═══════════════════════════════════════════════════════════════════════╗
# ║  Extract IDs from Scrape JSON Files                                   ║
# ║                                                                       ║
# ║  Usage:                                                               ║
# ║    • From inside the "scrapes" folder:                                ║
# ║        python extract_ids.py                                          ║
# ║                                                                       ║
# ║  The script will:                                                     ║
# ║    - List all scrape JSON files (e.g., copycord_1234_20250903-235914) ║
# ║    - Ask you to pick one by number                                    ║
# ║    - Extract all unique 'id' fields                                   ║
# ║    - Save them to <filename>_ids.txt in the same folder               ║
# ╚═══════════════════════════════════════════════════════════════════════╝

class IDExtractor:
    """Extracts 'id' fields from a JSON structure, deduplicated in original order."""
    def extract_ids(self, data: Any) -> List[str]:
        flat_ids = list(self._walk_for_ids(data))
        seen = set()
        uniq: List[str] = []
        for v in flat_ids:
            if v not in seen:
                seen.add(v)
                uniq.append(v)
        return uniq

    def _walk_for_ids(self, obj: Any) -> Iterable[str]:
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == "id":
                    try:
                        yield str(v)
                    except Exception:
                        pass
                else:
                    yield from self._walk_for_ids(v)
        elif isinstance(obj, list):
            for item in obj:
                yield from self._walk_for_ids(item)
        # Other types ignored


class ScrapePicker:
    """Finds scrape JSON files in a directory and prompts the user to pick one."""
    # Example pattern: server_123456789123456789_20250903-235914.json
    PATTERN = re.compile(r".+_\d+_\d{8}-\d{6}\.json$", re.IGNORECASE)

    def __init__(self, directory: Path) -> None:
        self.directory = directory

    def list_scrapes(self) -> List[Path]:
        candidates = sorted(self.directory.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        return [p for p in candidates if self.PATTERN.match(p.name)]

    def prompt_choice(self, files: List[Path]) -> Path:
        if not files:
            raise FileNotFoundError(
                f"No scrape files found in '{self.directory.resolve()}'. "
                "Expected names like 'server_123456789123456789_20250903-235914.json'."
            )

        print(f"Found {len(files)} scrape file(s) in {self.directory.resolve()}:")
        for i, f in enumerate(files, start=1):
            size_kb = f.stat().st_size / 1024
            print(f"  {i}. {f.name}  ({size_kb:.1f} KB)")

        while True:
            choice = input(f"\nPick a number (1-{len(files)}) or 0 to cancel: ").strip()
            if choice.isdigit():
                n = int(choice)
                if n == 0:
                    raise KeyboardInterrupt("User canceled.")
                if 1 <= n <= len(files):
                    return files[n - 1]
            print("Invalid selection. Please try again.")


def write_ids_to_txt(ids: List[str], out_path: Path) -> None:
    out_path.write_text("\n".join(ids) + ("\n" if ids else ""), encoding="utf-8")
    print(f"\n✅ Wrote {len(ids)} unique IDs to: {out_path}")


def resolve_scrapes_dir(arg_dir: str | None) -> Path:
    """Pick a sensible directory:
       - If --dir provided, use it.
       - Else prefer ./scrapes if it exists, otherwise current directory.
    """
    if arg_dir:
        d = Path(arg_dir)
        if not d.is_dir():
            raise FileNotFoundError(f"--dir path does not exist or is not a directory: {d}")
        return d

    here = Path.cwd()
    scrapes_sub = here / "scrapes"
    return scrapes_sub if scrapes_sub.is_dir() else here


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract all 'id' values from a scrape JSON into a text file.")
    parser.add_argument(
        "--dir",
        default=None,
        help="Directory containing scrape JSON files. "
             "If omitted, uses ./scrapes if it exists, else the current directory.",
    )
    args = parser.parse_args()

    scrapes_dir = resolve_scrapes_dir(args.dir)
    picker = ScrapePicker(scrapes_dir)
    files = picker.list_scrapes()

    try:
        chosen = picker.prompt_choice(files)
    except KeyboardInterrupt:
        print("\nCanceled.")
        return

    print(f"\nParsing: {chosen.name}")
    try:
        data = json.loads(chosen.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"❌ Failed to read/parse JSON: {e}")
        return

    extractor = IDExtractor()
    ids = extractor.extract_ids(data)

    out_path = chosen.with_name(chosen.stem + "_ids.txt")
    try:
        write_ids_to_txt(ids, out_path)
    except Exception as e:
        print(f"❌ Failed to write output: {e}")


if __name__ == "__main__":
    main()
