"""
MIT License

Copyright (c) 2021 vcokltfre

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from pathlib import Path
from json import load, dump
from hashlib import sha256
from schema import Schema, Optional
from string import printable
from functools import lru_cache

from typing import Union


dbschema = Schema({
    "filemap": {
        Optional(str): {
            Optional(str): str,
        },
    },
})


class DBError(Exception):
    pass


class OhNoDB:
    def __init__(self, path: Union[Path, str]):
        if isinstance(path, str):
            path = Path(path)

        self.path: Path = path

        self.filemap: dict
        self.size: int

        self.files = {}

        if self.path.exists():
            self._open()
        else:
            self._init()

    @staticmethod
    @lru_cache(1000)
    def valid_table_name(name: str) -> bool:
        return all([c in printable for c in name])

    def _open(self) -> None:
        with (self.path / "db.json").open(encoding="utf-8") as f:
            try:
                data = load(f)
            except:
                raise DBError("Database is corrupt. [JSON LOADING ERROR]") from None

        if not dbschema.is_valid(data):
            raise DBError("Database is corrupt. [SCHEMA VALIDATION ERROR]") from None

        self.filemap = data["filemap"]

        for table, items in self.filemap.items():
            for name, filename in items.items():
                if not (self.path / "files" / table / filename).exists():
                    raise DBError(f"Database is corrupt. [STARTUP FILE NOT FOUND: {table}/{name}:{filename}]")

    def _init(self) -> None:
        self.path.mkdir(parents=True)
        (self.path / "files").mkdir()

        with (self.path / "db.json").open("w", encoding="utf-8") as f:
            dump({"filemap":{}}, f)

        self.filemap = {}

    def _save(self) -> None:
        with (self.path / "db.json").open("w", encoding="utf-8") as f:
            dump({"filemap": self.filemap}, f)

    def create(self, table: str, name: str, data, is_json: bool = True) -> None:
        if not self.valid_table_name(table):
            raise DBError("Invalid table name.")

        tp = (self.path / "files" / table)
        tp.mkdir(exist_ok=True)

        filename = sha256(name.encode("utf-8")).hexdigest()

        fp = (tp / filename)

        if fp.exists():
            raise DBError(f"File already exists: {table}/{name} ({filename})")

        with fp.open("w", encoding="utf-8") as f:
            if is_json:
                try:
                    dump(data, f)
                except:
                    raise DBError("Invalid JSON data.") from None
            else:
                f.write(data)

        self.files[f"{table}/{filename}"] = data

        if not table in self.filemap:
            self.filemap[table] = {name: filename}
        else:
            self.filemap[table][name] = filename

        self._save()

    def fetch(self, table: str, name: str, is_json: bool = True) -> Union[str, dict, list]:
        if not self.valid_table_name(table):
            raise DBError("Invalid table name.")

        filename = sha256(name.encode("utf-8")).hexdigest()

        if data := self.files.get(f"{table}/{filename}"):
            return data

        file = self.filemap.get(table, {}).get(name)

        if not file:
            raise DBError("Database is corrupt. [MAPPED FILE NOT FOUND]")

        fp = (self.path / "files" / table / file)

        with fp.open(encoding="utf-8") as f:
            if is_json:
                try:
                    return load(f)
                except:
                    raise DBError("Invalid JSON data.") from None
            else:
                return f.read()

    def update(self, table: str, name: str, data, is_json: bool = True):
        if not self.valid_table_name(table):
            raise DBError("Invalid table name.")

        file = self.filemap.get(table, {}).get(name)

        if not file:
            raise DBError("Item not found.")

        fp = (self.path / "files" / table / file)

        with fp.open("w", encoding="utf-8") as f:
            if is_json:
                try:
                    dump(data, f)
                except:
                    raise DBError("Invalid JSON data.") from None
            else:
                f.write(data)
