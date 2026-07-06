import json
import logging
import os
from datetime import datetime

from pyspark.sql.functions import col

from src.validation.thresholds import ENTITY_CONFIG

logger = logging.getLogger(__name__)


def _generate_run_id() -> str:
    """Generate a run ID from the current timestamp."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def check_schema(df, expected_columns):
    """Check that the DataFrame contains exactly the expected columns."""
    actual = set(df.columns)
    expected = set(expected_columns)
    missing = expected - actual
    extra = actual - expected
    if missing or extra:
        return {
            "check": "schema",
            "status": "FAIL",
            "detail": (
                f"Missing columns: {sorted(missing)}; "
                f"Extra columns: {sorted(extra)}"
            ),
        }
    return {
        "check": "schema",
        "status": "PASS",
        "detail": f"All {len(expected_columns)} expected columns present",
    }


def check_non_empty(df):
    """Check that the DataFrame has at least one row."""
    count = df.count()
    if count == 0:
        return {"check": "non_empty", "status": "FAIL", "detail": "DataFrame is empty"}
    return {"check": "non_empty", "status": "PASS", "detail": f"Row count: {count}"}


def check_row_count_min(df, min_rows):
    """Check that the DataFrame has at least `min_rows` rows."""
    count = df.count()
    if count < min_rows:
        return {
            "check": "row_count_min",
            "status": "WARN",
            "detail": f"Row count {count} < minimum {min_rows}",
        }
    return {
        "check": "row_count_min",
        "status": "PASS",
        "detail": f"Row count {count} >= {min_rows}",
    }


def check_null_rate(df, columns, max_rate):
    """Check that null rate for each column does not exceed max_rate."""
    total = df.count()
    if total == 0:
        return {
            "check": "null_rate",
            "status": "FAIL",
            "detail": "Cannot compute null rate on empty DataFrame",
        }
    violations = []
    for col_name in columns:
        null_count = df.filter(col(col_name).isNull()).count()
        rate = null_count / total
        if rate > max_rate:
            violations.append({"column": col_name, "null_rate": round(rate, 4)})
    if violations:
        return {
            "check": "null_rate",
            "status": "WARN",
            "detail": (
                f"Columns exceeding {max_rate:.0%} null rate: {violations}"
            ),
        }
    return {
        "check": "null_rate",
        "status": "PASS",
        "detail": (
            f"All {len(columns)} columns within "
            f"{max_rate:.0%} null rate threshold"
        ),
    }


def check_duplicates(df, pk, max_rate):
    """Check that duplicate rate on PK does not exceed max_rate."""
    total = df.count()
    if total == 0:
        return {
            "check": "duplicates",
            "status": "FAIL",
            "detail": "Cannot compute duplicate rate on empty DataFrame",
        }
    dup_count = df.groupby(*pk).count().filter(col("count") > 1).count()
    rate = dup_count / total
    if rate > max_rate:
        return {
            "check": "duplicates",
            "status": "WARN",
            "detail": (
                f"Duplicate rate {rate:.2%} exceeds {max_rate:.0%}"
            ),
        }
    return {
        "check": "duplicates",
        "status": "PASS",
        "detail": (
            f"Duplicate rate {rate:.2%} within threshold"
        ),
    }


def _build_report(checks, entity_name, layer, run_id=None):
    """Build the full report dict from individual check results."""
    if run_id is None:
        run_id = _generate_run_id()
    total = len(checks)
    passed = sum(1 for c in checks if c["status"] == "PASS")
    failed = sum(1 for c in checks if c["status"] == "FAIL")
    warnings = sum(1 for c in checks if c["status"] == "WARN")
    return {
        "run_id": run_id,
        "layer": layer,
        "entity": entity_name,
        "timestamp": datetime.now().isoformat(),
        "checks": checks,
        "summary": {
            "total": total,
            "passed": passed,
            "failed": failed,
            "warnings": warnings,
        },
    }


def _save_report(report, output_dir):
    """Save the report as a JSON file under output_dir/layer/."""
    layer = report["layer"]
    entity = report["entity"]
    run_id = report["run_id"]
    dir_path = os.path.join(output_dir, layer)
    os.makedirs(dir_path, exist_ok=True)
    file_path = os.path.join(dir_path, f"{entity}_{run_id}.json")
    with open(file_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    logger.info("Quality report saved: %s", file_path)


def run_all(df, entity_name, layer, output_dir, run_id=None):
    """Run all configured quality checks for an entity and save the report.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Data to validate.
    entity_name : str
        Entity identifier (e.g. 'veiculos', 'posicoes_enriquecidas').
    layer : str
        Pipeline layer ('bronze', 'silver', 'gold').
    output_dir : str
        Base directory for quality reports.
    run_id : str, optional
        Run identifier. Auto-generated from timestamp if not provided.

    Returns
    -------
    dict or None
        The full report dict, or None if no config was found.
    """
    cfg = ENTITY_CONFIG.get(f"{layer}::{entity_name}")
    if not cfg:
        logger.warning(
            "No validation config for %s::%s, skipping", layer, entity_name
        )
        return None

    checks = [
        check_non_empty(df),
        check_schema(df, cfg["expected_columns"]),
        check_row_count_min(df, cfg["min_rows"]),
        check_null_rate(df, cfg["expected_columns"], cfg["null_rate_max"]),
        check_duplicates(df, cfg["pk"], cfg["duplicate_rate_max"]),
    ]

    report = _build_report(checks, entity_name, layer, run_id)
    _save_report(report, output_dir)

    failed = report["summary"]["failed"]
    if failed > 0:
        logger.warning(
            "Validation for %s::%s: %d check(s) failed",
            layer, entity_name, failed,
        )

    return report
