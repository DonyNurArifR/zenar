"""A database written by an earlier build must open and give the same answers.

The fixture in tests/fixtures/db_fixture was written by a build with TurboVec 1.0.0 and the record format
version that stores a 32-bit field count. Changing the index or the on-disk format must keep this test passing, or
the change must regenerate the fixture on purpose and say so in the changelog.
"""

import json
import pathlib
import shutil
import tempfile

import zenar

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "db_fixture"


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
        # Compare the sets, not the order. Documents with near-identical scores can swap places
        # between CPU paths (NEON, AVX2), and the expected file was written on Apple Silicon.
        assert sorted(h["id"] for h in hits) == sorted(case["ids"])
    db.close()
