from flask import Flask, jsonify
from databricks import sql
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Set up Flask app
app = Flask(__name__)

# Get Databricks connection info from environment variables
HOST = os.getenv('DATABRICKS_HOST')
HTTP_PATH = os.getenv('DATABRICKS_HTTP_PATH')
ACCESS_TOKEN = os.getenv('DATABRICKS_TOKEN')

@app.route('/')
def home():
    return "Backend connected to Databricks (soon!)"

@app.route('/test-connection')
def test_connection():
    try:
        with sql.connect(
            server_hostname=HOST,
            http_path=HTTP_PATH,
            access_token=ACCESS_TOKEN
        ) as connection:
            return jsonify({"status": "Connected to Databricks ✅"})
    except Exception as e:
        return jsonify({"status": "Failed", "error": str(e)}), 500

# Define route
@app.route('/data')
def get_data():
    try:
        # Connect to Databricks
        with sql.connect(
            server_hostname=HOST,
            http_path=HTTP_PATH,
            access_token=ACCESS_TOKEN
        ) as connection:

            # Open a cursor and run a query
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM gage_dev_databricks.gold_layer.customer LIMIT 10")
                rows = cursor.fetchall()

                # Convert result to a list of dicts
                columns = [desc[0] for desc in cursor.description]
                result = [dict(zip(columns, row)) for row in rows]

                return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Start Flask app
if __name__ == '__main__':
    app.run(debug=True)
