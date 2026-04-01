"""
Payment reconciliation engine.
Matches platform transactions against bank settlements and identifies gaps.
"""

import pandas as pd
from dataclasses import dataclass, field
from typing import List, Dict, Any
from datetime import datetime


@dataclass
class Discrepancy:
    gap_type: str
    transaction_id: str
    details: str
    platform_amount: float = 0.0
    bank_amount: float = 0.0
    variance: float = 0.0


@dataclass
class ReconciliationResult:
    total_transactions: int = 0
    total_settlements: int = 0
    matched: int = 0
    unmatched_transactions: int = 0
    unmatched_settlements: int = 0
    total_variance: float = 0.0
    rounding_total: float = 0.0
    discrepancies: List[Discrepancy] = field(default_factory=list)
    summary: Dict[str, int] = field(default_factory=dict)


def reconcile(transactions_path: str, settlements_path: str, month_end: str = "2025-01-31") -> ReconciliationResult:
    txn = pd.read_csv(transactions_path)
    stl = pd.read_csv(settlements_path)

    txn["date"] = pd.to_datetime(txn["date"])
    stl["settlement_date"] = pd.to_datetime(stl["settlement_date"])
    txn["amount"] = txn["amount"].astype(float)
    stl["amount"] = stl["amount"].astype(float)

    month_end_dt = pd.Timestamp(month_end) + pd.Timedelta(hours=23, minutes=59, seconds=59)

    result = ReconciliationResult(
        total_transactions=len(txn),
        total_settlements=len(stl),
    )

    discrepancies = []

    # --- 1. Detect duplicate settlements ---
    dup_counts = stl.groupby("transaction_id").size()
    duplicate_txn_ids = set(dup_counts[dup_counts > 1].index)

    for txn_id in duplicate_txn_ids:
        dup_rows = stl[stl["transaction_id"] == txn_id]
        count = len(dup_rows)
        amt = dup_rows["amount"].iloc[0]

        # Check if this txn_id even exists in transactions
        txn_match = txn[txn["transaction_id"] == txn_id]
        platform_amt = float(txn_match["amount"].iloc[0]) if len(txn_match) > 0 else 0.0

        discrepancies.append(Discrepancy(
            gap_type="duplicate_settlement",
            transaction_id=txn_id,
            details=f"Settled {count} times (expected 1). Extra settlement amount: ${abs(amt):.2f}",
            platform_amount=platform_amt,
            bank_amount=float(amt * count),
            variance=float(amt * (count - 1)),
        ))

    # Deduplicate settlements for further matching (keep first)
    stl_dedup = stl.drop_duplicates(subset="transaction_id", keep="first")

    # --- 2. Merge on transaction_id ---
    merged = txn.merge(stl_dedup, on="transaction_id", how="outer", suffixes=("_txn", "_stl"))

    # --- 3. Matched (amounts equal) ---
    matched_mask = merged["date"].notna() & merged["settlement_date"].notna()
    matched = merged[matched_mask]

    exact_match = matched[matched["amount_txn"] == abs(matched["amount_stl"])]
    result.matched = len(exact_match)

    # --- 4. Rounding differences ---
    amount_mismatch = matched[
        (matched["amount_txn"] != abs(matched["amount_stl"])) &
        (~matched["transaction_id"].isin(duplicate_txn_ids))
    ]

    for _, row in amount_mismatch.iterrows():
        platform_amt = float(row["amount_txn"])
        bank_amt = float(abs(row["amount_stl"]))
        diff = round(platform_amt - bank_amt, 2)

        discrepancies.append(Discrepancy(
            gap_type="rounding_difference",
            transaction_id=row["transaction_id"],
            details=f"Platform: ${platform_amt:.2f}, Bank: ${bank_amt:.2f}, Diff: ${diff:.2f}",
            platform_amount=platform_amt,
            bank_amount=bank_amt,
            variance=diff,
        ))

    # --- 5. Unmatched transactions (in platform, not in bank within month) ---
    unmatched_txn = merged[merged["settlement_date"].isna() & merged["date"].notna()]

    for _, row in unmatched_txn.iterrows():
        discrepancies.append(Discrepancy(
            gap_type="unmatched_transaction",
            transaction_id=row["transaction_id"],
            details=f"Transaction on {row['date'].strftime('%Y-%m-%d')} with no settlement found",
            platform_amount=float(row["amount_txn"]),
            bank_amount=0.0,
            variance=float(row["amount_txn"]),
        ))

    # --- 6. Cross-month settlements ---
    cross_month = matched[
        (matched["date"] <= month_end_dt) &
        (matched["settlement_date"] > month_end_dt)
    ]

    for _, row in cross_month.iterrows():
        discrepancies.append(Discrepancy(
            gap_type="cross_month_settlement",
            transaction_id=row["transaction_id"],
            details=f"Transaction on {row['date'].strftime('%Y-%m-%d')}, settled {row['settlement_date'].strftime('%Y-%m-%d')} (next month)",
            platform_amount=float(row["amount_txn"]),
            bank_amount=float(abs(row["amount_stl"])),
            variance=0.0,
        ))

    # --- 7. Orphan settlements (in bank, not in platform) ---
    orphan_stl = merged[merged["date"].isna() & merged["settlement_date"].notna()]

    for _, row in orphan_stl.iterrows():
        discrepancies.append(Discrepancy(
            gap_type="orphan_settlement",
            transaction_id=row["transaction_id"],
            details=f"Bank settlement on {row['settlement_date'].strftime('%Y-%m-%d')} with no matching transaction. Amount: ${abs(row['amount_stl']):.2f}",
            platform_amount=0.0,
            bank_amount=float(abs(row["amount_stl"])),
            variance=float(row["amount_stl"]),
        ))

    result.discrepancies = discrepancies
    result.unmatched_transactions = len(unmatched_txn)
    result.unmatched_settlements = len(orphan_stl)
    result.total_variance = round(sum(d.variance for d in discrepancies), 2)
    result.rounding_total = round(
        sum(d.variance for d in discrepancies if d.gap_type == "rounding_difference"), 2
    )

    # Summary by gap type
    result.summary = {}
    for d in discrepancies:
        result.summary[d.gap_type] = result.summary.get(d.gap_type, 0) + 1

    return result


def result_to_dict(result: ReconciliationResult) -> Dict[str, Any]:
    return {
        "total_transactions": result.total_transactions,
        "total_settlements": result.total_settlements,
        "matched": result.matched,
        "unmatched_transactions": result.unmatched_transactions,
        "unmatched_settlements": result.unmatched_settlements,
        "total_variance": result.total_variance,
        "rounding_total": result.rounding_total,
        "summary": result.summary,
        "discrepancies": [
            {
                "gap_type": d.gap_type,
                "transaction_id": d.transaction_id,
                "details": d.details,
                "platform_amount": d.platform_amount,
                "bank_amount": d.bank_amount,
                "variance": d.variance,
            }
            for d in result.discrepancies
        ],
    }
