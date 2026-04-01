"""
Generate synthetic payment transaction and bank settlement data
with planted reconciliation gaps for testing.
"""

import csv
import random
import uuid
from datetime import datetime, timedelta

random.seed(42)

MERCHANTS = [f"MERCH_{i:03d}" for i in range(1, 21)]
CUSTOMERS = [f"CUST_{i:04d}" for i in range(1, 101)]

def random_date(start, end):
    delta = end - start
    random_days = random.randint(0, delta.days)
    random_seconds = random.randint(0, 86399)
    return start + timedelta(days=random_days, seconds=random_seconds)

def generate():
    jan_start = datetime(2025, 1, 1)
    jan_end = datetime(2025, 1, 31, 23, 59, 59)

    transactions = []
    settlements = []
    settlement_counter = 0

    # --- Normal transactions (180) ---
    for i in range(1, 181):
        txn_id = f"TXN_{i:04d}"
        txn_date = random_date(jan_start, jan_end)
        amount = round(random.uniform(5.00, 500.00), 2)
        txn_type = "payment"
        merchant = random.choice(MERCHANTS)
        customer = random.choice(CUSTOMERS)

        transactions.append({
            "transaction_id": txn_id,
            "date": txn_date.strftime("%Y-%m-%d %H:%M:%S"),
            "amount": amount,
            "type": txn_type,
            "merchant_id": merchant,
            "customer_id": customer,
        })

        # Settlement 1-2 days later
        settle_delay = random.randint(1, 2)
        settle_date = txn_date + timedelta(days=settle_delay)
        settlement_counter += 1

        settlements.append({
            "settlement_id": f"STL_{settlement_counter:04d}",
            "transaction_id": txn_id,
            "settlement_date": settle_date.strftime("%Y-%m-%d %H:%M:%S"),
            "amount": amount,
            "status": "settled",
        })

    # --- GAP 1: Cross-month timing (3 transactions on Jan 30-31, settle in Feb) ---
    for i in range(181, 184):
        txn_id = f"TXN_{i:04d}"
        day = random.choice([30, 31])
        txn_date = datetime(2025, 1, day, random.randint(8, 22), random.randint(0, 59))
        amount = round(random.uniform(50.00, 300.00), 2)

        transactions.append({
            "transaction_id": txn_id,
            "date": txn_date.strftime("%Y-%m-%d %H:%M:%S"),
            "amount": amount,
            "type": "payment",
            "merchant_id": random.choice(MERCHANTS),
            "customer_id": random.choice(CUSTOMERS),
        })

        # Settles in February
        settle_date = datetime(2025, 2, random.randint(1, 2), random.randint(8, 18))
        settlement_counter += 1
        settlements.append({
            "settlement_id": f"STL_{settlement_counter:04d}",
            "transaction_id": txn_id,
            "settlement_date": settle_date.strftime("%Y-%m-%d %H:%M:%S"),
            "amount": amount,
            "status": "settled",
        })

    # --- GAP 2: Rounding differences ---
    # Each transaction has a tiny sub-cent diff (e.g. bank processes $49.99 as $49.984)
    # Individually they round to the same cent, but when you sum all 15 of them
    # the platform total and bank total diverge by ~$0.15
    for i in range(184, 199):
        txn_id = f"TXN_{i:04d}"
        txn_date = random_date(jan_start, datetime(2025, 1, 28))
        # Platform records clean cent amounts
        amount = round(random.uniform(10.00, 100.00), 2)

        transactions.append({
            "transaction_id": txn_id,
            "date": txn_date.strftime("%Y-%m-%d %H:%M:%S"),
            "amount": amount,
            "type": "payment",
            "merchant_id": random.choice(MERCHANTS),
            "customer_id": random.choice(CUSTOMERS),
        })

        settle_delay = random.randint(1, 2)
        settle_date = txn_date + timedelta(days=settle_delay)
        settlement_counter += 1

        # Bank settles 1 cent less — each row is $0.01 off,
        # looks minor alone but sums to $0.15 total drift
        settlements.append({
            "settlement_id": f"STL_{settlement_counter:04d}",
            "transaction_id": txn_id,
            "settlement_date": settle_date.strftime("%Y-%m-%d %H:%M:%S"),
            "amount": round(amount - 0.01, 2),
            "status": "settled",
        })

    # --- GAP 3: Duplicate settlements (2 transactions get settled twice) ---
    duplicate_txns = ["TXN_0005", "TXN_0042"]
    for txn_id in duplicate_txns:
        # Find original settlement
        orig = next(s for s in settlements if s["transaction_id"] == txn_id)
        settlement_counter += 1
        settlements.append({
            "settlement_id": f"STL_{settlement_counter:04d}",
            "transaction_id": txn_id,
            "settlement_date": orig["settlement_date"],
            "amount": orig["amount"],
            "status": "settled",
        })

    # --- GAP 4: Orphan refund (refund in settlements with no matching transaction) ---
    settlement_counter += 1
    settlements.append({
        "settlement_id": f"STL_{settlement_counter:04d}",
        "transaction_id": "TXN_9999",
        "settlement_date": "2025-01-20 14:30:00",
        "amount": -75.50,
        "status": "refund",
    })

    # --- Also add a few normal refunds in transactions so they're not all payments ---
    for i in range(199, 205):
        txn_id = f"TXN_{i:04d}"
        txn_date = random_date(jan_start, datetime(2025, 1, 28))
        amount = round(random.uniform(10.00, 80.00), 2)

        transactions.append({
            "transaction_id": txn_id,
            "date": txn_date.strftime("%Y-%m-%d %H:%M:%S"),
            "amount": amount,
            "type": "refund",
            "merchant_id": random.choice(MERCHANTS),
            "customer_id": random.choice(CUSTOMERS),
        })

        settle_delay = random.randint(1, 2)
        settle_date = txn_date + timedelta(days=settle_delay)
        settlement_counter += 1
        settlements.append({
            "settlement_id": f"STL_{settlement_counter:04d}",
            "transaction_id": txn_id,
            "settlement_date": settle_date.strftime("%Y-%m-%d %H:%M:%S"),
            "amount": -amount,
            "status": "refund",
        })

    # Write CSVs
    with open("data/transactions.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["transaction_id", "date", "amount", "type", "merchant_id", "customer_id"])
        writer.writeheader()
        writer.writerows(sorted(transactions, key=lambda x: x["date"]))

    with open("data/settlements.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["settlement_id", "transaction_id", "settlement_date", "amount", "status"])
        writer.writeheader()
        writer.writerows(sorted(settlements, key=lambda x: x["settlement_date"]))

    print(f"Generated {len(transactions)} transactions and {len(settlements)} settlements")
    print("Planted gaps:")
    print("  - 3 cross-month settlements (TXN_0181-0183)")
    print("  - 15 rounding differences of $0.01 each (TXN_0184-0198), $0.15 total drift")
    print("  - 2 duplicate settlements (TXN_0005, TXN_0042)")
    print("  - 1 orphan refund (TXN_9999)")

if __name__ == "__main__":
    import os
    os.makedirs("data", exist_ok=True)
    generate()
