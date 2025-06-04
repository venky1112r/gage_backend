from flask import Flask, request, jsonify, make_response
from flask_cors import CORS, cross_origin
from databricks import sql
from dotenv import load_dotenv
import os
import jwt
import datetime

# Azure Key Vault imports
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient

# Load environment variables
load_dotenv()

app = Flask(__name__)
# FrontendOrigin = "http://172.172.147.218"
FrontendOrigin = "http://localhost:5173" 

CORS(app, supports_credentials=True, origins=[FrontendOrigin])

# Setup Azure Key Vault client
KEY_VAULT_URL = os.getenv("KEY_VAULT_URL")

credential = ClientSecretCredential(
        tenant_id =  os.getenv('TENANT_ID'),
        client_id =  os.getenv('CLIENT_ID'),
        client_secret =  os.getenv('CLIENT_SECRET')
)
secret_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)

# Retrieve secrets from Azure Key Vault
HOST = secret_client.get_secret("DATABRICKS-HOST").value
HTTP_PATH = secret_client.get_secret("DATABRICKS-HTTP-PATH").value
ACCESS_TOKEN = secret_client.get_secret("DATABRICKS-TOKEN").value


# JWT Setup
# SECRET_KEY = os.getenv("JWT_SECRET")
SECRET_KEY = secret_client.get_secret("JWT-SECRET").value
# print(SECRET_KEY)
JWT_EXP_DELTA_SECONDS = 300  # Token valid for 5 minutes

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
        response.headers["Access-Control-Allow-Origin"] = FrontendOrigin
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
                    SELECT password, userrole , plantid
                    FROM gage_dev_databricks.gold_layer.customer 
                    WHERE email = ?
                """, (email,))
                
                result = cursor.fetchone()

                if not result or password != result[0]:
                    return jsonify({"message": "Invalid email or password"}), 401

                userrole = result[1]
                plantid = result[2]
                token = generate_jwt(email)
                # print(f"Generated token: {token}")
                # print(f"User Role: {userrole}")

                response = jsonify({
                    "message": "Login successful",
                    "userrole": userrole,
                    "plantid": plantid,
                    "token": token  # Send token to frontend
                })
                response.headers["Access-Control-Allow-Origin"] = FrontendOrigin
                response.headers["Access-Control-Allow-Credentials"] = "true"
                return response
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ✅ Protected route using Authorization header
@app.route('/api/protected', methods=['GET'])
@cross_origin(origin=FrontendOrigin, supports_credentials=True)
def protected():
    auth_header = request.headers.get('Authorization')

    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"message": "Unauthorized - No token provided"}), 401

    token = auth_header.split(" ")[1]
    # print(f"Received token:", token)

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
    
@app.route('/api/dashboard-metrics', methods=['GET'])
def dashboard():
    try:
        with sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=ACCESS_TOKEN) as connection:
            with connection.cursor() as cursor:
                # Contracted CI Score
                cursor.execute("SELECT ROUND(AVG(ci_score_final_gc02e_per_MJ), 2)FROM gold_layer.dashboard_info;")
                ci_score = cursor.fetchone()[0]

                # Total Bushels
                cursor.execute("SELECT ROUND(SUM(contract_contractquantity), 2)FROM gold_layer.dashboard_info;")
                total_bushels = cursor.fetchone()[0]

                # Authorized Grower Percentage
                cursor.execute("""
                    SELECT ROUND(AVG(CASE WHEN contract_schedules_schedule_nameidtype = 'C' THEN 100.0 ELSE 0.0 END), 2)
FROM gold_layer.dashboard_info;
                """)
                authorized_grower = cursor.fetchone()[0]

                # Contract BI CI Score Level Delivered

                cursor.execute("""
                    SELECT
    CASE
        WHEN c.SupplierID = 'C' AND ci_score_final_gc02e_per_bu IS NOT NULL THEN 'Grower'
        WHEN c.SupplierID = 'G' AND ci_score_final_gc02e_per_bu IS NOT NULL THEN 'Retailer'
        WHEN c.SupplierID = 'C' AND ci_score_final_gc02e_per_bu IS NULL THEN 'No Score Grower'
        WHEN c.SupplierID = 'G' AND ci_score_final_gc02e_per_bu IS NULL THEN 'No Score Retailer'
        ELSE 'Other' -- Changed 'Custome' to 'Other' for clarity and common practice
    END AS customertype,
    ROUND(SUM(c.SuppliedQuantity),2) AS Bushels,
    ROUND(AVG(ci.ci_score_final_gc02e_per_MJ),2) CIScore
from gold.contractdata c
    left outer join bronze.cultura_ci ci on ci.producer_id=c.NameID
GROUP BY
    CASE
        WHEN c.SupplierID = 'C' AND ci_score_final_gc02e_per_bu IS NOT NULL THEN 'Grower'
        WHEN c.SupplierID = 'G' AND ci_score_final_gc02e_per_bu IS NOT NULL THEN 'Retailer'
        WHEN c.SupplierID = 'C' AND ci_score_final_gc02e_per_bu IS NULL THEN 'No Score Grower'
        WHEN c.SupplierID = 'G' AND ci_score_final_gc02e_per_bu IS NULL THEN 'No Score Retailer'
        ELSE 'Other'
END;
                """)
                contract_delivered_data = cursor.fetchall()
                
                contract_delivered = [{"nameidtype": row[0], "total_delivered": row[1], "ci_score": row[2]} for row in contract_delivered_data if len(row) >= 3]


                # Contract BI CI Score Level Pending 

                cursor.execute("""
                  SELECT
    CASE
        WHEN c.SupplierID = 'C' AND ci_score_final_gc02e_per_bu IS NOT NULL THEN 'Grower'
        WHEN c.SupplierID = 'G' AND ci_score_final_gc02e_per_bu IS NOT NULL THEN 'Retailer'
        WHEN c.SupplierID = 'C' AND ci_score_final_gc02e_per_bu IS NULL THEN 'No Score Grower'
        WHEN c.SupplierID = 'G' AND ci_score_final_gc02e_per_bu IS NULL THEN 'No Score Retailer'
        ELSE 'Other' -- Changed 'Custome' to 'Other' for clarity and common practice
    END AS customertype,
    ROUND(SUM(c.RemainingQuantity),2) AS Bushels,
    ROUND(AVG(ci.ci_score_final_gc02e_per_MJ),2) CIScore
from gold.contractdata c
    left outer join bronze.cultura_ci ci on ci.producer_id=c.NameID
GROUP BY
    CASE
        WHEN c.SupplierID = 'C' AND ci_score_final_gc02e_per_bu IS NOT NULL THEN 'Grower'
        WHEN c.SupplierID = 'G' AND ci_score_final_gc02e_per_bu IS NOT NULL THEN 'Retailer'
        WHEN c.SupplierID = 'C' AND ci_score_final_gc02e_per_bu IS NULL THEN 'No Score Grower'
        WHEN c.SupplierID = 'G' AND ci_score_final_gc02e_per_bu IS NULL THEN 'No Score Retailer'
        ELSE 'Other'
END;
                """)
                contract_pending_data = cursor.fetchall()
                contract_pending = [{"nameidtype": row[0], "total_pending": row[1], "ci_score": row[2]} for row in contract_pending_data if len(row) >= 3]


                # bushels by ci score delivered
                cursor.execute("""
                                        SELECT
                        CASE
                            WHEN contract_schedules_schedule_nameidtype = 'C' AND ci_score_final_gc02e_per_bu IS NOT NULL THEN 'Grower'
                            WHEN contract_schedules_schedule_nameidtype = 'G' AND ci_score_final_gc02e_per_bu IS NOT NULL THEN 'Retailer'
                            WHEN contract_schedules_schedule_nameidtype = 'C' AND ci_score_final_gc02e_per_bu IS NULL THEN 'No Score Grower'
                            WHEN contract_schedules_schedule_nameidtype = 'G' AND ci_score_final_gc02e_per_bu IS NULL THEN 'No Score Retailer'
                            ELSE 'Other' -- Changed 'Custome' to 'Other' for clarity and common practice
                        END AS customertype,
                        ROUND(SUM(contract_appliedquantity), 2) AS Bushels
                    FROM
                        gold_layer.dashboard_info
                    GROUP BY
                        CASE
                            WHEN contract_schedules_schedule_nameidtype = 'C' AND ci_score_final_gc02e_per_bu IS NOT NULL THEN 'Grower'
                            WHEN contract_schedules_schedule_nameidtype = 'G' AND ci_score_final_gc02e_per_bu IS NOT NULL THEN 'Retailer'
                            WHEN contract_schedules_schedule_nameidtype = 'C' AND ci_score_final_gc02e_per_bu IS NULL THEN 'No Score Grower'
                            WHEN contract_schedules_schedule_nameidtype = 'G' AND ci_score_final_gc02e_per_bu IS NULL THEN 'No Score Retailer'
                            ELSE 'Other'
                        END;
                """)
                delivered_data = cursor.fetchall()
            
                bushels_delivered = [{"role": row[0], "delivered": row[1], "ci_score": row[2]} for row in delivered_data if len(row) >= 3]

                # bushels by ci score Pending
                cursor.execute("""
                    SELECT
    CASE
        WHEN contract_schedules_schedule_nameidtype = 'C' AND ci_score_final_gc02e_per_bu IS NOT NULL THEN 'Grower'
        WHEN contract_schedules_schedule_nameidtype = 'G' AND ci_score_final_gc02e_per_bu IS NOT NULL THEN 'Retailer'
        WHEN contract_schedules_schedule_nameidtype = 'C' AND ci_score_final_gc02e_per_bu IS NULL THEN 'No Score Grower'
        WHEN contract_schedules_schedule_nameidtype = 'G' AND ci_score_final_gc02e_per_bu IS NULL THEN 'No Score Retailer'
        ELSE 'Other' -- Changed 'Custome' to 'Other' for clarity and common practice
    END AS customertype,
    ROUND(SUM(contract_remainingquantity), 2) AS Bushels
FROM
    gold_layer.dashboard_info
GROUP BY
    CASE
        WHEN contract_schedules_schedule_nameidtype = 'C' AND ci_score_final_gc02e_per_bu IS NOT NULL THEN 'Grower'
        WHEN contract_schedules_schedule_nameidtype = 'G' AND ci_score_final_gc02e_per_bu IS NOT NULL THEN 'Retailer'
        WHEN contract_schedules_schedule_nameidtype = 'C' AND ci_score_final_gc02e_per_bu IS NULL THEN 'No Score Grower'
        WHEN contract_schedules_schedule_nameidtype = 'G' AND ci_score_final_gc02e_per_bu IS NULL THEN 'No Score Retailer'
        ELSE 'Other'
    END;
                """)
                pending_data = cursor.fetchall()
                bushels_pending = [{"role": row[0], "pending": row[1], "ci_score": row[2]} for row in pending_data if len(row) >= 3]

            # summary card data 
                cursor.execute("""
                    SELECT 
                contractedciscore,
                contractedbushels,
                rebate,
                authorizedgrowers
                FROM gold_layer.metadata;
                """)           
                summary_data = cursor.fetchall()
                summary = [{
                    "contracted_ci_score": row[0],
                    "contracted_bushels": row[1],
                    "rebate": row[2],
                    "authorized_growers": row[3]
                } for row in summary_data]
                
                # print(f"Summary Data:", summary_data)

        return jsonify({
            "contracted_ci_score": ci_score,
            "total_bushels": total_bushels,
            "authorized_grower_percentage": authorized_grower,
             "contract_ci_score_level_delivered": contract_delivered,
            "contract_ci_score_level_pending": contract_pending,
            "bushels_ci_score_level_delivered": bushels_delivered,
            "bushels_ci_score_level_pending": bushels_pending,
            "summary": summary
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/setting/manual-input', methods=['GET', 'POST'])
def manual_input_handler():
    if request.method == 'GET':
        plant_id = request.args.get('plantid')  # Get plantid from query string if provided
        print("Received GET request with plantid:", plant_id)

        try:
            with sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=ACCESS_TOKEN) as connection:
                with connection.cursor() as cursor:
                    if plant_id:
                        # print("Received GET request with plantid:", plant_id)
                        query = "SELECT * FROM gold_layer.plantinfo WHERE plantid = ?"
                        cursor.execute(query, (plant_id,))
                    else:
                        # print("Received GET request without plantid")
                        query = "SELECT * FROM gold_layer.plantinfo"
                        cursor.execute(query)

                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    result = [dict(zip(columns, row)) for row in rows]
                    return jsonify(result)
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    elif request.method == 'POST':
        print("Received POST request" + str(request.get_json()))
        try:
            data = request.get_json()

            # Validate required fields (you can customize as needed)
            required_fields = [
                 "plantid", "totalbushelsprocessed", "totalethanolproduced",
                "gridelectricusage", "renewablelectricusage", "fossilgasused", "coalusage",
                "naturalgasrenewable45z", "convefficiency", "createdate", "updateddate", "updatedby"
            ]
            for field in required_fields:
                if field not in data:
                    return jsonify({"error": f"Missing field: {field}"}), 400

            with sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=ACCESS_TOKEN) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO gold_layer.plantinfo (
                             plantid, totalbushelsprocessed, totalethanolproduced,
                            gridelectricusage, renewablelectricusage, fossilgasused, coalusage,
                            naturalgasrenewable45z, convefficiency, createdate, updateddate,
                            updatedon, updatedby
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,  ?, ?, ?, ?)
                    """, (
                        data["plantid"],
                        data["totalbushelsprocessed"],
                        data["totalethanolproduced"],
                        data["gridelectricusage"],
                        data["renewablelectricusage"],
                        data["fossilgasused"],
                        data["coalusage"],
                        data["naturalgasrenewable45z"],
                        data["convefficiency"],
                        data["createdate"],
                        data["updateddate"],
                        data.get("updatedon"),  # Can be None
                        data["updatedby"]
                    ))

            return jsonify({"status": "Manual plant input inserted successfully ✅"})

        except Exception as e:
            return jsonify({"error": str(e)}), 500
  


# Run app
if __name__ == '__main__':
    app.run(debug=True, port=3000, host="localhost")
    # app.run(debug=True, port=3000, host="0.0.0.0")
