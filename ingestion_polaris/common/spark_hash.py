"""Exact reproduction of Spark's `creative_url_hash` for the CTV/Digital occurrence flow.

Legacy (PySpark) computes, in the staging->raw step:

    CAST(xxhash64(CONCAT(
        SPLIT_PART(creative.url, '.', 1), '.',
        SPLIT_PART(creative.url, '.', 2), '.',
        SPLIT_PART(creative.url, '.', -1))) AS BIGINT)

Trino's built-in xxhash64 uses seed 0 (and returns varbinary), so it CANNOT reproduce Spark's
value. Spark's `xxhash64` is the standard XXH64 over the UTF-8 bytes with **seed 42**. We therefore
precompute the hash here, at landing, and carry it as a column in the staging table; the dbt-trino
staging->raw model just passes it through (no re-hash in SQL).

Parity verified against real PySpark 3.5 `xxhash64` for the exact expression above (all test URLs
matched, incl. split_part edge cases). See the Piece-1 notes in docs/.
"""
import ctypes

import xxhash


def _to_signed_int64(u: int) -> int:
    """XXH64 returns an unsigned 64-bit int; Spark BIGINT is signed. Reinterpret the bits."""
    return ctypes.c_int64(u).value


def split_part(s, delimiter: str, part: int):
    """Replicate Spark's SPLIT_PART: 1-based index, negative counts from the end, out-of-range
    yields '' (empty string), and a NULL input yields NULL."""
    if s is None:
        return None
    parts = s.split(delimiter)
    n = len(parts)
    if part > 0:
        return parts[part - 1] if part - 1 < n else ""
    if part < 0:
        return parts[n + part] if n + part >= 0 else ""
    return ""  # part == 0 is undefined in Spark; not used by the CTV expression


def url_hash_input(url):
    """Build the exact CONCAT(...) string Spark hashes. Spark CONCAT returns NULL if any arg is
    NULL; here the three SPLIT_PART results are non-NULL whenever `url` is non-NULL."""
    if url is None:
        return None
    return (
        split_part(url, ".", 1) + "." + split_part(url, ".", 2) + "." + split_part(url, ".", -1)
    )


def creative_url_hash(url):
    """Return the signed-int64 hash Spark would produce for `creative.url` (or None if url is None)."""
    concat = url_hash_input(url)
    if concat is None:
        return None
    return _to_signed_int64(xxhash.xxh64(concat.encode("utf-8"), seed=42).intdigest())
