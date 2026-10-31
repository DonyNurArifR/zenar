# Walkthrough of the agent flow: the schema, valid and invalid filters, warnings, the prompt,
# and regeneration after a write. Needs jsonschema: pip install jsonschema
# Run: python examples/agent_walkthrough.py
import json, tempfile
import jsonschema
from zenar import VectorDB

db = VectorDB(tempfile.mkdtemp())
c = db.create_collection("products", dimension=8)
rows = [
    ("wrench", "A", "tools", 12.5, True),
    ("kettle", "A", "appliances", 40.0, True),
    ("radio", "B", "electronics", 25.0, False),
    ("hammer", "B", "tools", 18.0, True),
]
c.add_many([{"id": i, "vector": [float(i + 1)] * 8, "text": t,
             "metadata": {"tenant": ten, "category": cat, "price": p, "in_stock": s}}
            for i, (t, ten, cat, p, s) in enumerate(rows, start=1)])

SCOPE = None  # a single-tenant catalogue; a multi-tenant app passes {"tenant": ...} here
spec = c.tool_spec("search_products", scope=SCOPE)
schema = spec["parameters"]
where_schema = schema["properties"]["where"]

# 1. Is the argument schema a valid JSON Schema?
jsonschema.Draft202012Validator.check_schema(schema)
print("1. argument schema is valid JSON Schema: OK")

# 2. Filters a model would write. Each must validate against the schema, and get the expected result.
good = [
    ({"tenant": "A"}, 2),
    ({"tenant": "B", "in_stock": True}, 1),
    ({"price": {"gte": 15, "lt": 30}}, 2),
    ({"category": {"in": ["tools", "electronics"]}}, 3),
    ({"or": [{"category": "tools"}, {"price": {"gt": 35}}]}, 3),
    ({"not": {"tenant": "A"}}, 2),
    ({}, 4),
]
for where, expected in good:
    jsonschema.validate({"query": "x", "where": where}, schema)
    r = c.retrieve([1.0] * 8, top_k=10, where=where or None, scope=SCOPE)
    assert r["matched"] == expected, (where, r["matched"], expected)
print(f"2. {len(good)} valid filters validate against the schema and return the expected counts: OK")

# 3. Mistakes a model might make. Each must fail with a message the model can act on.
mistakes = [
    ({"catgory": "tools"}, "did you mean \"category\""),
    ({"price": "cheap"}, "expected a number"),
    ({"price": {"between": [1, 2]}}, "unknown operator \"between\""),
    ({"tenant": {}}, "needs at least one operator"),
    ({"in_stock": "yes"}, "expected true or false"),
    ({"and": "tenant=A"}, "list of where objects"),
]
for where, fragment in mistakes:
    try:
        c.retrieve([1.0] * 8, where=where, scope=SCOPE)
        raise AssertionError(f"accepted a bad filter: {where}")
    except Exception as e:
        assert fragment in str(e), (where, str(e))
        print(f"3. {json.dumps(where):<34} -> {type(e).__name__}: {e}")

# 4. A value that does not exist: zero hits and a warning, not an error.
r = c.retrieve([1.0] * 8, where={"tenant": "Z"}, scope=SCOPE)
assert r["hits"] == [] and r["matched"] == 0 and r["warnings"]
print("4. unknown value:", r["warnings"][0])

# 5. Results are plain JSON, as a tool result must be.
r = c.retrieve([1.0] * 8, top_k=2, where={"tenant": "A"}, scope=SCOPE)
json.dumps(r)
print("5. tool result serializes to JSON:", json.dumps(r)[:110], "...")

# 6. The prompt carries the real fields and values.
prompt = spec.create_prompt(instructions="You help customers.")
for needle in ['- tenant (text): values "A", "B"', "- price (float):", '- in_stock (boolean)', "You help customers."]:
    assert needle in prompt, needle
print("6. prompt lists the live fields and values: OK")

# 7. The spec is a snapshot: new values appear only after regenerating it.
before = c.tool_spec("search_products", scope=SCOPE).create_prompt()
assert '"C"' not in before
c.add(5, [9.0] * 8, "lamp", {"tenant": "C", "category": "electronics", "price": 9.0, "in_stock": True})
assert '"C"' not in before, "an old prompt must not change"
assert '"C"' in c.tool_spec("search_products", scope=SCOPE).create_prompt()
print("7. regenerating the spec after a write picks up the new tenant: OK")
# 8. Tenant isolation: a scoped agent cannot see or reach another tenant.
scoped = {"tenant": "A"}
assert c.retrieve([1.0] * 8, where={"tenant": "B"}, scope=scoped)["matched"] == 0
assert c.retrieve([1.0] * 8, where={"or": [{"tenant": "A"}, {"tenant": "B"}]}, scope=scoped)["matched"] == 2
assert "\"B\"" not in str(c.describe(scope=scoped))
assert "\"B\"" not in c.tool_spec("search_products", scope=scoped).create_prompt()
print("8. tenant scope holds against where, or, and discovery: OK")
db.close()
print("ALL CHECKS PASSED")
