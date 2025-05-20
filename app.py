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
JWT_EXP_DELTA_SECONDS = 300  # 5 minutes

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

# ✅ Login Route with JWT returned in response
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
                cursor.execute(f"""
                    SELECT password, userrole 
                    FROM gage_dev_databricks.gold_layer.customer 
                    WHERE email = ?
                """, (email,))
                
                result = cursor.fetchone()

                if not result or password != result[0]:
                    return jsonify({"message": "Invalid email or password"}), 401

                userrole = result[1]
                token = generate_jwt(email)
                print(f"Generated token: {token}")
                print(f"User Role: {userrole}")

                response = jsonify({
                    "message": "Login successful",
                    "userrole": userrole,
                    "token": token  # Send token to frontend
                })
                response.headers["Access-Control-Allow-Origin"] = "http://localhost:5173"
                response.headers["Access-Control-Allow-Credentials"] = "true"
                return response
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ✅ Protected route using Authorization header
@app.route('/api/protected', methods=['GET'])
@cross_origin(origin="http://localhost:5173", supports_credentials=True)
def protected():
    auth_header = request.headers.get('Authorization')

    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"message": "Unauthorized - No token provided"}), 401

    token = auth_header.split(" ")[1]
    print(f"Received token:", token)

    try:
        decoded_token = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return jsonify({"message": "Protected route access granted"})
    except jwt.ExpiredSignatureError:
        return jsonify({"message": "Token expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"message": "Invalid token"}), 401

# ✅ Logout route (optional)
# @app.route("/api/logout", methods=["POST"])
# def logout():
#     response = make_response(jsonify({"message": "Logged out"}))
#     response.set_cookie(
#         "token",
#         "",
#         expires=0,
#         httponly=True,
#         samesite="Lax",
#         secure=False
#     )
#     return response

@app.route('/insert-user', methods=['POST'])
def insert_user():
    try:
        data = request.json

        # Validate required fields (optional, for safety)
        required_fields = [
            "customerid", "customername", "source", "customertype", "erp", "plantname",
            "plantid", "locationname", "locationid", "firstname", "lastname", "userid",
            "email", "userrole", "createddate", "modifydate", "password"
        ]
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing field: {field}"}), 400

        # Connect and insert
        with sql.connect(
            server_hostname=HOST,
            http_path=HTTP_PATH,
            access_token=ACCESS_TOKEN
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    INSERT INTO gage_dev_databricks.gold_layer.customer (
                        customerid, customername, source, customertype, erp,
                        plantname, plantid, locationname, locationid, firstname,
                        lastname, userid, email, userrole, createddate, modifydate, password
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                """, (
                    data['customerid'], data['customername'], data['source'], data['customertype'], data['erp'],
                    data['plantname'], data['plantid'], data['locationname'], data['locationid'], data['firstname'],
                    data['lastname'], data['userid'], data['email'], data['userrole'],
                    data['createddate'], data['modifydate'], data['password']
                ))

        return jsonify({"status": "User inserted successfully ✅"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/delete-user', methods=['DELETE'])
def delete_user():
    data = request.get_json()
    print("Delete request data:", data)

    if not data or 'userrole' not in data:
        return jsonify({"error": "Missing field: userrole"}), 400

    userrole = data['userrole']

    try:
        with sql.connect(
            server_hostname=HOST,
            http_path=HTTP_PATH,
            access_token=ACCESS_TOKEN
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM gage_dev_databricks.gold_layer.customer
                    WHERE userrole = ?
                """, (userrole,))
        
        return jsonify({"status": f"User with userid '{userrole}' deleted successfully ✅"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Run app
if __name__ == '__main__':
    app.run(debug=True, port=3000, host="localhost")
