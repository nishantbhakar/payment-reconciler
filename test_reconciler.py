"""
Test cases for the reconciliation engine.
Verifies each gap type is correctly detected.
"""

import pytest
import os
import csv
import tempfile
from reconciler import reconcile


@pytest.fixture
def data_dir():
    d = tempfile.mkdtemp()
    return d


def write_csv(path, fieldnames, rows):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


TXN_FIELDS = ["transaction_id", "date", "amount", "type", "merchant_id", "customer_id"]
STL_FIELDS = ["settlement_id", "transaction_id", "settlement_date", "amount", "status"]


def test_exact_match(data_dir):
    """All transactions match perfectly — no discrepancies."""
    txn_path = os.path.join(data_dir, "txn.csv")
    stl_path = os.path.join(data_dir, "stl.csv")

    write_csv(txn_path, TXN_FIELDS, [
        {"transaction_id": "T1", "date": "2025-01-10 10:00:00", "amount": 100.00, "type": "payment", "merchant_id": "M1", "customer_id": "C1"},
        {"transaction_id": "T2", "date": "2025-01-15 12:00:00", "amount": 50.00, "type": "payment", "merchant_id": "M1", "customer_id": "C2"},
    ])
    write_csv(stl_path, STL_FIELDS, [
        {"settlement_id": "S1", "transaction_id": "T1", "settlement_date": "2025-01-11 10:00:00", "amount": 100.00, "status": "settled"},
        {"settlement_id": "S2", "transaction_id": "T2", "settlement_date": "2025-01-16 12:00:00", "amount": 50.00, "status": "settled"},
    ])

    result = reconcile(txn_path, stl_path)
    assert result.matched == 2
    assert len(result.discrepancies) == 0


def test_cross_month_settlement(data_dir):
    """Transaction in January, settlement in February — detected as cross-month gap."""
    txn_path = os.path.join(data_dir, "txn.csv")
    stl_path = os.path.join(data_dir, "stl.csv")

    write_csv(txn_path, TXN_FIELDS, [
        {"transaction_id": "T1", "date": "2025-01-31 22:00:00", "amount": 200.00, "type": "payment", "merchant_id": "M1", "customer_id": "C1"},
    ])
    write_csv(stl_path, STL_FIELDS, [
        {"settlement_id": "S1", "transaction_id": "T1", "settlement_date": "2025-02-01 10:00:00", "amount": 200.00, "status": "settled"},
    ])

    result = reconcile(txn_path, stl_path)
    cross = [d for d in result.discrepancies if d.gap_type == "cross_month_settlement"]
    assert len(cross) == 1
    assert cross[0].transaction_id == "T1"


def test_rounding_difference(data_dir):
    """Bank settles 1 cent less — detected as rounding difference."""
    txn_path = os.path.join(data_dir, "txn.csv")
    stl_path = os.path.join(data_dir, "stl.csv")

    write_csv(txn_path, TXN_FIELDS, [
        {"transaction_id": "T1", "date": "2025-01-10 10:00:00", "amount": 19.99, "type": "payment", "merchant_id": "M1", "customer_id": "C1"},
    ])
    write_csv(stl_path, STL_FIELDS, [
        {"settlement_id": "S1", "transaction_id": "T1", "settlement_date": "2025-01-11 10:00:00", "amount": 19.98, "status": "settled"},
    ])

    result = reconcile(txn_path, stl_path)
    rounding = [d for d in result.discrepancies if d.gap_type == "rounding_difference"]
    assert len(rounding) == 1
    assert rounding[0].variance == 0.01


def test_duplicate_settlement(data_dir):
    """Same transaction settled twice — detected as duplicate."""
    txn_path = os.path.join(data_dir, "txn.csv")
    stl_path = os.path.join(data_dir, "stl.csv")

    write_csv(txn_path, TXN_FIELDS, [
        {"transaction_id": "T1", "date": "2025-01-10 10:00:00", "amount": 100.00, "type": "payment", "merchant_id": "M1", "customer_id": "C1"},
    ])
    write_csv(stl_path, STL_FIELDS, [
        {"settlement_id": "S1", "transaction_id": "T1", "settlement_date": "2025-01-11 10:00:00", "amount": 100.00, "status": "settled"},
        {"settlement_id": "S2", "transaction_id": "T1", "settlement_date": "2025-01-11 10:00:00", "amount": 100.00, "status": "settled"},
    ])

    result = reconcile(txn_path, stl_path)
    dups = [d for d in result.discrepancies if d.gap_type == "duplicate_settlement"]
    assert len(dups) == 1
    assert dups[0].transaction_id == "T1"


def test_orphan_settlement(data_dir):
    """Refund in bank with no matching transaction — detected as orphan."""
    txn_path = os.path.join(data_dir, "txn.csv")
    stl_path = os.path.join(data_dir, "stl.csv")

    write_csv(txn_path, TXN_FIELDS, [
        {"transaction_id": "T1", "date": "2025-01-10 10:00:00", "amount": 50.00, "type": "payment", "merchant_id": "M1", "customer_id": "C1"},
    ])
    write_csv(stl_path, STL_FIELDS, [
        {"settlement_id": "S1", "transaction_id": "T1", "settlement_date": "2025-01-11 10:00:00", "amount": 50.00, "status": "settled"},
        {"settlement_id": "S2", "transaction_id": "T_UNKNOWN", "settlement_date": "2025-01-20 14:00:00", "amount": -75.50, "status": "refund"},
    ])

    result = reconcile(txn_path, stl_path)
    orphans = [d for d in result.discrepancies if d.gap_type == "orphan_settlement"]
    assert len(orphans) == 1
    assert orphans[0].transaction_id == "T_UNKNOWN"


def test_multiple_gaps_combined(data_dir):
    """All gap types present at once — all detected correctly."""
    txn_path = os.path.join(data_dir, "txn.csv")
    stl_path = os.path.join(data_dir, "stl.csv")

    write_csv(txn_path, TXN_FIELDS, [
        # Normal match
        {"transaction_id": "T1", "date": "2025-01-05 10:00:00", "amount": 100.00, "type": "payment", "merchant_id": "M1", "customer_id": "C1"},
        # Cross-month
        {"transaction_id": "T2", "date": "2025-01-31 20:00:00", "amount": 200.00, "type": "payment", "merchant_id": "M1", "customer_id": "C2"},
        # Rounding
        {"transaction_id": "T3", "date": "2025-01-15 10:00:00", "amount": 49.99, "type": "payment", "merchant_id": "M2", "customer_id": "C3"},
        # Will be duplicated in settlements
        {"transaction_id": "T4", "date": "2025-01-12 08:00:00", "amount": 75.00, "type": "payment", "merchant_id": "M1", "customer_id": "C4"},
    ])
    write_csv(stl_path, STL_FIELDS, [
        {"settlement_id": "S1", "transaction_id": "T1", "settlement_date": "2025-01-06 10:00:00", "amount": 100.00, "status": "settled"},
        {"settlement_id": "S2", "transaction_id": "T2", "settlement_date": "2025-02-01 10:00:00", "amount": 200.00, "status": "settled"},
        {"settlement_id": "S3", "transaction_id": "T3", "settlement_date": "2025-01-16 10:00:00", "amount": 49.98, "status": "settled"},
        {"settlement_id": "S4", "transaction_id": "T4", "settlement_date": "2025-01-13 10:00:00", "amount": 75.00, "status": "settled"},
        {"settlement_id": "S5", "transaction_id": "T4", "settlement_date": "2025-01-13 10:00:00", "amount": 75.00, "status": "settled"},
        {"settlement_id": "S6", "transaction_id": "T_GHOST", "settlement_date": "2025-01-22 14:00:00", "amount": -30.00, "status": "refund"},
    ])

    result = reconcile(txn_path, stl_path)
    types_found = {d.gap_type for d in result.discrepancies}

    assert "cross_month_settlement" in types_found
    assert "rounding_difference" in types_found
    assert "duplicate_settlement" in types_found
    assert "orphan_settlement" in types_found
    assert result.matched >= 1  # T1 should match
