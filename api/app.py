from flask import Flask, jsonify
import os
import psycopg2

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

@app.route("/health")
def health():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.close()
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/run/daily_sales", methods=["POST"])
def run_daily_sales():
    # Placeholder for now
    return jsonify({"status": "stub - will implement ETL here later"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)