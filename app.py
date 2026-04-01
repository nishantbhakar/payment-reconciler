"""
Flask web application for the payment reconciliation dashboard.
"""

from flask import Flask, render_template, jsonify, request
from reconciler import reconcile, result_to_dict
import os

app = Flask(__name__)

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


@app.route("/")
def dashboard():
    return render_template("dashboard.html")


@app.route("/api/reconcile")
def api_reconcile():
    txn_path = os.path.join(DATA_DIR, "transactions.csv")
    stl_path = os.path.join(DATA_DIR, "settlements.csv")

    if not os.path.exists(txn_path) or not os.path.exists(stl_path):
        return jsonify({"error": "Data files not found. Run generate_data.py first."}), 404

    result = reconcile(txn_path, stl_path)
    data = result_to_dict(result)

    # Optional filter
    gap_filter = request.args.get("gap_type")
    if gap_filter:
        data["discrepancies"] = [d for d in data["discrepancies"] if d["gap_type"] == gap_filter]

    return jsonify(data)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
