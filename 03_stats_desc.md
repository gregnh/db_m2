---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.0'
      jupytext_version: 1.0.5
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

```python
import sqlite3 as sqlite
```

```python
con = sqlite.connect("project_db.db")
cur = con.cursor()
```
