"""The `where` schema the model sees must never accept a filter the parser refuses."""

import random
import tempfile

import pytest

import zenar

jsonschema = pytest.importorskip("jsonschema")

FIELDS = ["category", "price", "in_stock", "tenant", "nope"]
VALUES = ["kitchen", "tools", "x", 5, 150.0, True, None, [1], {"gt": 3}, {"lt": 50},
          {"in": ["tools"]}, {"eq": "kitchen"}, {"bogus": 1}, ""]


def _random_where(rng, depth=0):
    where = {}
    for _ in range(rng.randint(0, 3)):
        roll = rng.random()
        if depth < 2 and roll < 0.1:
            where["and"] = [_random_where(rng, depth + 1) for _ in range(rng.randint(0, 2))]
        elif depth < 2 and roll < 0.15:
            where["or"] = [_random_where(rng, depth + 1) for _ in range(rng.randint(0, 2))]
        elif depth < 2 and roll < 0.2:
            where["not"] = _random_where(rng, depth + 1)
        else:
            where[rng.choice(FIELDS)] = rng.choice(VALUES)
    return where


def test_every_where_the_schema_accepts_the_parser_accepts():
    rng = random.Random(9)
    db = zenar.VectorDB(tempfile.mkdtemp())
    c = db.create_collection("p", dimension=3)
    c.add_many([
        {"id": i, "vector": [1.0, 0.0, 0.0], "text": "t", "metadata": {
            "category": rng.choice(["kitchen", "tools", "garden"]),
            "price": float(rng.randint(1, 200)), "in_stock": rng.random() < 0.5, "tenant": "A"}}
        for i in range(120)
    ])
    params = c.tool_spec("search", scope={"tenant": "A"})["parameters"]
    promised_but_refused = []
    for _ in range(1500):
        where = _random_where(rng)
        try:
            jsonschema.validate({"query": "q", "where": where}, params)
            schema_accepts = True
        except jsonschema.ValidationError:
            schema_accepts = False
        if not schema_accepts:
            continue
        try:
            c.retrieve([1.0, 0.0, 0.0], where=where, scope={"tenant": "A"})
        except zenar.InvalidFilterError:
            promised_but_refused.append(where)
    assert promised_but_refused == [], promised_but_refused[:3]
    db.close()
