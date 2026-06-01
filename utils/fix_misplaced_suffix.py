"""One-off repair: move the tag suffix (-C/-U/-UC) from the end of a filename onto
the code component, undoing an earlier replace bug.

  ABW-123.女优.[标题]-UC.mp4  ->  ABW-123-UC.女优.[标题].mp4

Renames the file on disk AND updates videos.original_file_path so DB and disk stay
consistent. Safe: skips when source is missing or target already exists. Pass
--apply to execute; default is a dry run.
"""
import argparse
import os
import re
import sqlite3
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from config import DB_PATH

SUFFIX_END = re.compile(r'-(UC|U|C)$', re.I)


def compute_fix(name: str):
    """Return the corrected filename, or None if no misplaced suffix is present."""
    dot = name.rfind('.')
    stem, ext = (name[:dot], name[dot:]) if dot > 0 else (name, '')
    comps = stem.split('.')
    if len(comps) < 2:
        return None
    trailing = None
    for i in range(1, len(comps)):
        m = SUFFIX_END.search(comps[i])
        if m:
            trailing = '-' + m.group(1).upper()
            comps[i] = SUFFIX_END.sub('', comps[i])
    if not trailing:
        return None
    comps[0] = SUFFIX_END.sub('', comps[0]) + trailing
    return '.'.join(c for c in comps if c) + ext


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--apply', action='store_true', help='actually rename (default: dry run)')
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        'SELECT id, original_file_path FROM videos WHERE deleted=0'
    ).fetchall()

    renamed = skipped = 0
    for r in rows:
        old_path = r['original_file_path']
        if not old_path:
            continue
        old = Path(old_path)
        new_name = compute_fix(old.name)
        if not new_name or new_name == old.name:
            continue
        new = old.parent / new_name

        if not old.exists():
            print(f'SKIP (source missing) id={r["id"]}: {old}')
            skipped += 1
            continue
        if new.exists():
            print(f'SKIP (target exists)  id={r["id"]}: {new}')
            skipped += 1
            continue

        print(f'{"RENAME" if args.apply else "DRY"} id={r["id"]}: {old.name}\n          -> {new_name}')
        if args.apply:
            old.rename(new)
            conn.execute(
                'UPDATE videos SET original_file_path = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
                (str(new), r['id']),
            )
            conn.commit()
        renamed += 1

    conn.close()
    print(f'\n{"Renamed" if args.apply else "Would rename"}: {renamed}, skipped: {skipped}')


if __name__ == '__main__':
    main()
