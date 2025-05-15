from flask import Flask, request, jsonify, make_response
from flask_cors import CORS, cross_origin
from databricks import sql
from dotenv import load_dotenv
import os
import jwt
import datetime

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Enable CORS
CORS(app, supports_credentials=True, origins=["http://localhost:5173"])

# Databricks connection
HOST = os.getenv('DATABRICKS_HOST')
HTTP_PATH = os.getenv('DATABRICKS_HTTP_PATH')
ACCESS_TOKEN = os.getenv('DATABRICKS_TOKEN')

# JWT setup
SECRET_KEY = os.getenv("JWT_SECRET")
JWT_EXP_DELTA_SECONDS = 3600

def generate_jwt(email):
    payload = {
        "email": email,
        "exp": datetime.datetime.utcnow() + datetime.timedelta(seconds=JWT_EXP_DELTA_SECONDS)
    }
    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")

def decode_jwt(token):
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None

@app.route('/')
def home():
    return "Flask backend for GAGE is running"

@app.route('/test-connection')
def test_connection():
    try:
        with sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=ACCESS_TOKEN) as connection:
            return jsonify({"status": "Connected to Databricks ✅"})
    except Exception as e:
        return jsonify({"status": "Failed", "error": str(e)}), 500

@app.route('/data')
def get_data():
    try:
        with sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=ACCESS_TOKEN) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM gage_dev_databricks.gold_layer.customer LIMIT 10")
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                result = [dict(zip(columns, row)) for row in rows]
                return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ✅ Login Route with JWT cookie
@app.route('/api/login', methods=['POST', 'OPTIONS'])
def login():
    if request.method == 'OPTIONS':
        response = make_response()
        response.headers["Access-Control-Allow-Origin"] = "http://localhost:5173"
        response.headers["Access-Control-Allow-Credentials"] = "true"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"
        response.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        return response

    data = request.get_json()
    email = data.get("email")
    password = data.get("password")

    if not email or not password:
        return jsonify({"message": "Email and password are required"}), 400

    try:
        with sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=ACCESS_TOKEN) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT password FROM gage_dev_databricks.gold_layer.customer WHERE email = '{email}'")
                result = cursor.fetchone()

                if not result or password != result[0]:  # Replace with hash check
                    return jsonify({"message": "Invalid email or password"}), 401

                token = generate_jwt(email)
                print(f"Generated token: {token}")  # Log the token for debugging
                response = make_response(jsonify({"message": "Login successful"}))
                response.set_cookie(
                    "token",
                    token,
                    httponly=True,
                    samesite="None",
                    secure=True  # set to True in production with HTTPS
                )
                response.headers["Access-Control-Allow-Origin"] = "http://localhost:5173"
                response.headers["Access-Control-Allow-Credentials"] = "true"
                return response
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ✅ Protected route
@app.route('/api/protected', methods=['GET'])
@cross_origin(origin="http://localhost:5173", supports_credentials=True)
def protected():

    token = request.cookies.get('token')  # Get the token from the cookie
    print(f"Received token1:", token)  # Log the token for debugging

    if not token:
        return jsonify({"message": "Unauthorized - No token provided"}), 401

    try:
        decoded_token = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return jsonify({"message": "Protected route access granted"})
    except jwt.ExpiredSignatureError:
        return jsonify({"message": "Token expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"message": "Invalid token"}), 401



# ✅ Logout route (optional)
@app.route("/api/logout", methods=["POST"])
def logout():
    response = make_response(jsonify({"message": "Logged out"}))
    response.set_cookie("token", "", expires=0, httponly=True)
    response.headers["Access-Control-Allow-Origin"] = "http://localhost:5173"
    response.headers["Access-Control-Allow-Credentials"] = "true"
    return response

# Run app
if __name__ == '__main__':
    app.run(debug=True, port=3000, host="localhost")
