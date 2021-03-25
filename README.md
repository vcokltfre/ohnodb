# ohnodb

## Please never use this in production. I beg you, don't.

Usage:

```py
from ohnodb import OhNoDB

db = OhNoDB("./data")

my_data = {
    "hello":"world"
}

db.create("my_table", "my_item", my_data, is_json=True)

print(db.fetch("my_table", "my_item", is_json=True))  # >>> {"hello": "world"}

db.update("my_table", "my_item", {}, is_json=True)

print(db.fetch("my_table", "my_item", is_json=True))  # >>> {}
```
