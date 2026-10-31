"""A database written by an earlier build must open and give the same answers.

The fixture in tests/fixtures/db_turbovec_1_0_0 was written by Zenar 0.1.2 (TurboVec 1.0.0). If a change
to the index or its file format breaks this test, the change must say so and bump the fixture on purpose.
"""

import json
import pathlib
import shutil
import tempfile

import zenar

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "db_turbovec_1_0_0"


def _copy_fixture():
    root = pathlib.Path(tempfile.mkdtemp()) / "db"
    shutil.copytree(FIXTURE, root, ignore=shutil.ignore_patterns("expected.json"))
    return str(root)


def test_fixture_opens_with_the_same_count_and_answers():
    expected = json.loads((FIXTURE / "expected.json").read_text())
    db = zenar.VectorDB(_copy_fixture())
    c = db.get_collection("docs")
    assert c.count() == expected["count"]
    for case in expected["queries"]:
        hits = c.search(case["query"], top_k=5, tenant="A")
        assert [h["id"] for h in hits] == case["ids"]
    db.close()
