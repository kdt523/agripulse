"""Tiny Spark stand-in for testing orchestration/alerting/catalogue glue without a JVM."""

from __future__ import annotations

import re


class Row(dict):
    def __getitem__(self, k):
        return list(self.values())[k] if isinstance(k, int) else dict.__getitem__(self, k)

    def asDict(self):
        return dict(self)


class FakeWriter:
    def __init__(self, owner, rows):
        self.owner, self.rows, self._mode = owner, rows, None

    def format(self, _):
        return self

    def mode(self, m):
        self._mode = m
        return self

    def saveAsTable(self, name):
        self.owner.saved.append((name, self._mode, self.rows))


class FakeDF:
    def __init__(self, rows=None, owner=None, columns=None):
        self.rows = [Row(r) for r in (rows or [])]
        self.owner = owner
        self.columns = columns or (list(self.rows[0]) if self.rows else [])

    def collect(self):
        return self.rows

    def count(self):
        return len(self.rows)

    def limit(self, n):
        return FakeDF(self.rows[:n], self.owner, self.columns)

    def createOrReplaceTempView(self, _):
        return None

    @property
    def write(self):
        return FakeWriter(self.owner, self.rows)


class FakeCatalog:
    def __init__(self, existing):
        self.existing = set(existing)

    def tableExists(self, name):
        return name in self.existing


class FakeSpark:
    """handlers: [(regex, rows | callable(query) -> rows)] matched against spark.sql() text."""

    def __init__(self, handlers=None, tables=None, existing=()):
        self.handlers, self.tables = handlers or [], tables or {}
        self.sql_log: list[str] = []
        self.saved: list = []
        self.catalog = FakeCatalog(existing)

    def sql(self, q):
        self.sql_log.append(q)
        for pat, rows in self.handlers:
            if re.search(pat, q, re.S | re.I):
                return FakeDF(rows(q) if callable(rows) else rows, self)
        return FakeDF([], self)

    def table(self, name):
        return FakeDF(self.tables.get(name, []), self)

    def createDataFrame(self, rows, schema=None):
        names = [p.strip().split()[0] for p in schema.split(",")] if isinstance(schema, str) else None
        data = [dict(zip(names, r, strict=False)) if names and not isinstance(r, dict) else r for r in rows]
        return FakeDF(data, self, names)
