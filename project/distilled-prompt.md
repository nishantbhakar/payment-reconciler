# Distilled Prompt

> Build a full-stack payment reconciliation tool. Here are the requirements:
>
> **Data Generation:** Create a Python script that generates two CSV files:
> - `transactions.csv` — ~200 platform transactions for January 2025 with fields: transaction_id, date, amount, type (payment/refund), merchant_id, customer_id
> - `settlements.csv` — corresponding bank settlements with fields: settlement_id, transaction_id, settlement_date, amount, status
>
> Plant these specific discrepancies in the data:
> 1. 2-3 transactions from Jan 30-31 that settle in February (cross-month timing gap)
> 2. Systematic rounding differences on ~10 transactions (e.g., bank settles $19.98 instead of $19.99) that only become visible when summed
> 3. 1-2 duplicate entries in the settlements file (same transaction_id settled twice)
> 4. 1 refund in settlements with a transaction_id that doesn't exist in transactions (orphan refund)
>
> **Reconciliation Engine:** Python module that:
> - Matches transactions to settlements by transaction_id
> - Detects and categorizes all 4 gap types
> - Produces a summary report with totals, matched count, and gap breakdown
>
> **Web Dashboard:** A clean web UI (Flask + HTML/CSS/JS) that shows:
> - Summary cards: total transactions, matched, unmatched, total variance
> - A table of all discrepancies with gap type, transaction details, and amounts
> - Ability to filter by gap type
>
> **Tests:** pytest test cases that verify each gap type is correctly identified by the reconciliation engine.
>
> **Assumptions:** Single currency (USD), unique transaction IDs, settlements happen 1-2 days after transaction, no partial settlements, month-end cutoff is midnight UTC Jan 31.
>
> Make it deployable. Include a requirements.txt and clear folder structure.
