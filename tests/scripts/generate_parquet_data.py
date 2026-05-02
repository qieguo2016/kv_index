#!/usr/bin/env python3
"""Generate privacy-safe synthetic Parquet data for kv_index tests.

The generated schema intentionally uses generic column names and reserved
domains. It covers every scalar type supported by kv_index plus list versions
of those types, including multiple list<string> fields.

Examples:
  python3 tests/scripts/generate_parquet_data.py --output /tmp/kv_index.parquet
  python3 tests/scripts/generate_parquet_data.py --output /tmp/small.parquet --rows 1000
  python3 tests/scripts/generate_parquet_data.py --output /tmp/one_mb.parquet --target-size-mb 1 --seed 42
  python3 tests/scripts/generate_parquet_data.py --preview /tmp/kv_index.parquet --head 10
"""

from __future__ import annotations

import argparse
import json
import os
import random
import string
import sys
import tempfile
from pathlib import Path
from typing import Any


DEFAULT_TARGET_SIZE_MB = 1024
DEFAULT_ROW_GROUP_SIZE = 16_384
DEFAULT_SEED = 20260502
CALIBRATION_ROWS = 4096

TAG_WORDS = (
    "alpha",
    "bravo",
    "delta",
    "echo",
    "focus",
    "global",
    "harbor",
    "index",
    "juno",
    "kilo",
    "lumen",
    "matrix",
    "nova",
    "orbit",
    "pixel",
    "query",
    "relay",
    "signal",
    "tempo",
    "vector",
)


def _load_pyarrow() -> tuple[Any, Any]:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ModuleNotFoundError as exc:
        if exc.name != "pyarrow":
            raise
        print(
            "error: pyarrow is required to write Parquet files.\n"
            "Install it in this Python environment, for example:\n"
            "  python3 -m pip install pyarrow",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc
    return pa, pq


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return parsed


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic generic-schema Parquet data for kv_index tests.",
        epilog=(
            "If --rows is omitted, a small calibration file is written first and "
            "generation continues in bounded row groups until the target size is reached."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--output",
        help="Path to the Parquet file to create.",
    )
    mode_group.add_argument(
        "--preview",
        help="Path to an existing Parquet file to inspect instead of generating data.",
    )
    parser.add_argument(
        "--head",
        type=_non_negative_int,
        default=5,
        help="Rows to print when previewing a Parquet file.",
    )
    parser.add_argument(
        "--target-size-mb",
        type=_positive_int,
        default=DEFAULT_TARGET_SIZE_MB,
        help="Approximate output size when --rows is omitted.",
    )
    parser.add_argument(
        "--rows",
        type=_non_negative_int,
        help="Write exactly this many rows instead of targeting a file size.",
    )

    size_group = parser.add_mutually_exclusive_group()
    size_group.add_argument(
        "--batch-size",
        type=_positive_int,
        dest="row_group_size",
        default=DEFAULT_ROW_GROUP_SIZE,
        help="Rows generated and written at a time.",
    )
    size_group.add_argument(
        "--row-group-size",
        type=_positive_int,
        dest="row_group_size",
        help="Alias for --batch-size.",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help="Seed for deterministic synthetic data.",
    )
    return parser.parse_args(argv)


def build_schema(pa: Any) -> Any:
    return pa.schema(
        [
            pa.field("record_id", pa.uint64(), nullable=False),
            pa.field("small_code", pa.int8()),
            pa.field("count_i32", pa.int32()),
            pa.field("metric_i64", pa.int64()),
            pa.field("hash_u64", pa.uint64()),
            pa.field("is_enabled", pa.bool_()),
            pa.field("name_text", pa.string()),
            pa.field("resource_url", pa.string()),
            pa.field("media_url", pa.string()),
            pa.field("asset_url", pa.string()),
            pa.field("codes_i8", pa.list_(pa.int8())),
            pa.field("counts_i32", pa.list_(pa.int32())),
            pa.field("metrics_i64", pa.list_(pa.int64())),
            pa.field("hashes_u64", pa.list_(pa.uint64())),
            pa.field("flags_bool", pa.list_(pa.bool_())),
            pa.field("tags", pa.list_(pa.string())),
            pa.field("keywords", pa.list_(pa.string())),
            pa.field("labels", pa.list_(pa.string())),
        ]
    )


def maybe_null(rng: random.Random, value: Any, probability: float) -> Any:
    if rng.random() < probability:
        return None
    return value


def synthetic_word(rng: random.Random, prefix: str, max_suffix: int = 1_000_000) -> str:
    return f"{prefix}_{rng.choice(TAG_WORDS)}_{rng.randrange(max_suffix):06d}"


def synthetic_path(rng: random.Random) -> str:
    token = "".join(rng.choices(string.ascii_lowercase + string.digits, k=18))
    shard = rng.randrange(256)
    return f"{shard:02x}/{token}"


def synthetic_list(
    rng: random.Random,
    make_value: Any,
    *,
    max_length: int,
    null_probability: float = 0.08,
    empty_probability: float = 0.12,
) -> list[Any] | None:
    if rng.random() < null_probability:
        return None
    if rng.random() < empty_probability:
        return []
    return [make_value() for _ in range(rng.randint(1, max_length))]


def build_batch(pa: Any, schema: Any, rng: random.Random, start_row: int, row_count: int) -> Any:
    rows: dict[str, list[Any]] = {field.name: [] for field in schema}

    for row_offset in range(row_count):
        ordinal = start_row + row_offset
        random_id = rng.getrandbits(63)
        record_id = (((ordinal + 1) << 16) ^ random_id) & ((1 << 64) - 1)
        path = synthetic_path(rng)

        rows["record_id"].append(record_id)
        rows["small_code"].append(maybe_null(rng, rng.randint(-128, 127), 0.04))
        rows["count_i32"].append(maybe_null(rng, rng.randint(-2_000_000_000, 2_000_000_000), 0.05))
        rows["metric_i64"].append(maybe_null(rng, rng.randint(-(2**62), 2**62 - 1), 0.05))
        rows["hash_u64"].append(maybe_null(rng, rng.getrandbits(64), 0.04))
        rows["is_enabled"].append(maybe_null(rng, bool(rng.getrandbits(1)), 0.03))
        rows["name_text"].append(maybe_null(rng, synthetic_word(rng, "item"), 0.04))
        rows["resource_url"].append(
            maybe_null(rng, f"https://example.invalid/resource/{path}", 0.04)
        )
        rows["media_url"].append(
            maybe_null(rng, f"https://media.example.invalid/object/{path}.bin", 0.06)
        )
        rows["asset_url"].append(
            maybe_null(rng, f"https://cdn.example.invalid/assets/{path}.dat", 0.06)
        )

        rows["codes_i8"].append(
            synthetic_list(rng, lambda: rng.randint(-128, 127), max_length=8)
        )
        rows["counts_i32"].append(
            synthetic_list(
                rng,
                lambda: rng.randint(-1_000_000, 1_000_000),
                max_length=10,
            )
        )
        rows["metrics_i64"].append(
            synthetic_list(
                rng,
                lambda: rng.randint(-(2**40), 2**40),
                max_length=8,
            )
        )
        rows["hashes_u64"].append(
            synthetic_list(rng, lambda: rng.getrandbits(64), max_length=8)
        )
        rows["flags_bool"].append(
            synthetic_list(rng, lambda: bool(rng.getrandbits(1)), max_length=12)
        )
        rows["tags"].append(
            synthetic_list(rng, lambda: synthetic_word(rng, "tag", 10_000), max_length=6)
        )
        rows["keywords"].append(
            synthetic_list(rng, lambda: synthetic_word(rng, "kw", 10_000), max_length=8)
        )
        rows["labels"].append(
            synthetic_list(rng, lambda: synthetic_word(rng, "label", 10_000), max_length=5)
        )

    return pa.Table.from_pydict(rows, schema=schema)


def parquet_size_for_sample(pa: Any, pq: Any, schema: Any, seed: int, rows: int) -> int:
    sample_rng = random.Random(seed)
    sample = build_batch(pa, schema, sample_rng, 0, rows)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        pq.write_table(sample, tmp_path)
        return os.path.getsize(tmp_path)
    finally:
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass


def write_parquet(args: argparse.Namespace) -> None:
    pa, pq = _load_pyarrow()
    schema = build_schema(pa)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    rng = random.Random(args.seed)
    total_rows = 0
    target_bytes = args.target_size_mb * 1024 * 1024
    estimated_rows: int | None = None

    if args.rows is None:
        sample_rows = min(CALIBRATION_ROWS, args.row_group_size)
        sample_size = max(1, parquet_size_for_sample(pa, pq, schema, args.seed, sample_rows))
        estimated_rows = max(1, int(target_bytes / (sample_size / sample_rows)))

    with pq.ParquetWriter(output, schema) as writer:
        while True:
            if args.rows is not None:
                remaining = args.rows - total_rows
                if remaining <= 0:
                    break
                row_count = min(args.row_group_size, remaining)
            else:
                current_size = output.stat().st_size if output.exists() else 0
                if current_size >= target_bytes and total_rows > 0:
                    break
                row_count = args.row_group_size

            table = build_batch(pa, schema, rng, total_rows, row_count)
            writer.write_table(table, row_group_size=row_count)
            total_rows += row_count

    final_size = output.stat().st_size
    size_mb = final_size / (1024 * 1024)
    estimate_text = (
        f", estimated_rows={estimated_rows}" if estimated_rows is not None else ""
    )
    print(
        f"wrote {total_rows} rows to {output} "
        f"({final_size} bytes, {size_mb:.2f} MiB{estimate_text})"
    )


def preview_parquet(args: argparse.Namespace) -> None:
    parquet_path = Path(args.preview)
    if not parquet_path.exists():
        print(f"error: Parquet file does not exist: {parquet_path}", file=sys.stderr)
        raise SystemExit(1)

    _, pq = _load_pyarrow()
    parquet_file = pq.ParquetFile(parquet_path)
    metadata = parquet_file.metadata
    schema = parquet_file.schema_arrow

    print(f"file: {parquet_path}")
    print(f"rows: {metadata.num_rows}")
    print(f"columns: {metadata.num_columns}")
    print(f"row_groups: {metadata.num_row_groups}")
    print("schema:")
    for field in schema:
        print(f"  - {field.name}: {field.type}, nullable={field.nullable}")

    print(f"head({args.head}):")
    if args.head == 0:
        return

    remaining = args.head
    for batch in parquet_file.iter_batches(batch_size=min(args.head, 1024)):
        table = batch.to_pydict()
        batch_rows = batch.num_rows
        column_names = batch.schema.names
        for row_index in range(batch_rows):
            row = {name: table[name][row_index] for name in column_names}
            print(json.dumps(row, sort_keys=True))
            remaining -= 1
            if remaining <= 0:
                return


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.preview is not None:
        preview_parquet(args)
    else:
        write_parquet(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
