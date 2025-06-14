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
                select r.roleid,r.role,u.password,u.erpid,u.plantid
from gold.userinfo as u inner join gold.rolemasterinfo as r on u.roleid=r.roleid  WHERE u.username = ?
                """, (email,))
                
                result = cursor.fetchone()
                if not result or password != result[2]:
                    return jsonify({"message": "Invalid email or password"}), 401

                userrole = result[1]
                erpid = result[3]
                plantid = result[4]
                token = generate_jwt(email)
               
                response = jsonify({
                    "message": "Login successful",
                    "userrole": userrole,
                    "plantid": plantid,
                    "erpid": erpid,
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

# -------------------- PASSWORD RESET ENDPOINT --------------------

@app.route("/api/reset-password-request", methods=["POST"])
def send_password_reset_email():
    data = request.get_json()
    customer_id = data.get("customerId")

    # Dummy function - replace with actual implementation
    customer = get_customer_by_id(customer_id)
    if not customer:
        return jsonify({"message": "Customer not found"}), 404

    # Generate token
    token = generate_reset_token(customer_id)

    # Save token (implement save_token securely)
    save_token(customer_id, token, expiry_minutes=30)

    # Construct reset link
    reset_link = f"https://yourfrontend.com/reset-password?token={token}"

    # Send email (make sure render_template and send_email are defined properly)
    send_email(
        to=customer["email"],
        subject="Reset your G.A.G.E. password",
        html=render_template("reset_email.html", customer_name=customer["name"], reset_link=reset_link)
    )

    return jsonify({"message": "Reset email sent"})

# -------------------- UTILITIES --------------------

def generate_reset_token(customer_id):
    payload = {
        "customer_id": customer_id,
        "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=30)
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    return token

# Dummy functions for demonstration
def get_customer_by_id(customer_id):
    # You should query Databricks or your database
    return {"id": customer_id, "email": "customer@example.com", "name": "Customer Name"}

def save_token(customer_id, token, expiry_minutes=30):
    # Save to your database or Redis with expiry
    pass

def send_email(to, subject, html):
    # Implement using SendGrid, SMTP, etc.
    print(f"Sending email to {to} with subject {subject}")
    
@app.route('/api/dashboard-metrics', methods=['GET'])
def dashboard():
    try:
        with sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=ACCESS_TOKEN) as connection:
            with connection.cursor() as cursor:
                # Contracted CI Score
                # cursor.execute("SELECT ROUND(AVG(ci_score_final_gc02e_per_MJ), 2)FROM gold_layer.dashboard_info;")
                # ci_score = cursor.fetchone()[0]

                # Total Bushels
                # cursor.execute("SELECT ROUND(SUM(contract_contractquantity), 2)FROM gold_layer.dashboard_info;")
                # total_bushels = cursor.fetchone()[0]

                # Authorized Grower Percentage
#                 cursor.execute("""
#                     SELECT ROUND(AVG(CASE WHEN contract_schedules_schedule_nameidtype = 'C' THEN 100.0 ELSE 0.0 END), 2)
# FROM gold_layer.dashboard_info;
#                 """)
#                 authorized_grower = cursor.fetchone()[0]

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
            # "contracted_ci_score": ci_score,
            # "total_bushels": total_bushels,
            # "authorized_grower_percentage": authorized_grower,
             "contract_ci_score_level_delivered": contract_delivered,
            "contract_ci_score_level_pending": contract_pending,
            # "bushels_ci_score_level_delivered": bushels_delivered,
            # "bushels_ci_score_level_pending": bushels_pending,
            "summary": summary
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/dashboard/summary-metrics', methods=['GET'])
def summary_metrics():
    try:
        with sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=ACCESS_TOKEN) as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        contractedciscore,
                        contractedbushels,
                        rebate,
                        authorizedgrowers
                    FROM gold.dashboardinfo;
                """)
                rows = cursor.fetchall()
                summary = [{
                    "contracted_ci_score": row[0],
                    "contracted_bushels": row[1],
                    "rebate": row[2],
                    "authorized_growers": row[3]
                } for row in rows]
        return jsonify(summary)
    except Exception as e:
        return jsonify({"error": str(e)}), 500    
    
@app.route('/dashboard/contract-ci-score-level', methods=['GET'])
def contract_ci_score_level():
    
    try:
        with sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=ACCESS_TOKEN) as connection:
            with connection.cursor() as cursor:
                # Delivered
                cursor.execute("""
                    SELECT
                        CASE
                            WHEN c.SupplierID = 'C' AND ci_score_final_gc02e_per_bu IS NOT NULL THEN 'Grower'
                            WHEN c.SupplierID = 'G' AND ci_score_final_gc02e_per_bu IS NOT NULL THEN 'Retailer'
                            WHEN c.SupplierID = 'C' AND ci_score_final_gc02e_per_bu IS NULL THEN 'No Score Grower'
                            WHEN c.SupplierID = 'G' AND ci_score_final_gc02e_per_bu IS NULL THEN 'No Score Retailer'
                            ELSE 'Other'
                        END AS customertype,
                        ROUND(SUM(c.SuppliedQuantity),2) AS Bushels,
                        ROUND(AVG(ci.ci_score_final_gc02e_per_MJ),2) CIScore
                    FROM gold.contractdata c
                    LEFT OUTER JOIN bronze.cultura_ci ci ON ci.producer_id = c.NameID
                    GROUP BY 1
                """)
                delivered_data = cursor.fetchall()
                delivered = [{"nameidtype": row[0], "total_delivered": row[1], "ci_score": row[2]} for row in delivered_data]

                # Pending
                cursor.execute("""
                    SELECT
                        CASE
                            WHEN c.SupplierID = 'C' AND ci_score_final_gc02e_per_bu IS NOT NULL THEN 'Grower'
                            WHEN c.SupplierID = 'G' AND ci_score_final_gc02e_per_bu IS NOT NULL THEN 'Retailer'
                            WHEN c.SupplierID = 'C' AND ci_score_final_gc02e_per_bu IS NULL THEN 'No Score Grower'
                            WHEN c.SupplierID = 'G' AND ci_score_final_gc02e_per_bu IS NULL THEN 'No Score Retailer'
                            ELSE 'Other'
                        END AS customertype,
                        ROUND(SUM(c.RemainingQuantity),2) AS Bushels,
                        ROUND(AVG(ci.ci_score_final_gc02e_per_MJ),2) CIScore
                    FROM gold.contractdata c
                    LEFT OUTER JOIN bronze.cultura_ci ci ON ci.producer_id = c.NameID
                    GROUP BY 1
                """)
                pending_data = cursor.fetchall()
                pending = [{"nameidtype": row[0], "total_pending": row[1], "ci_score": row[2]} for row in pending_data]

        return jsonify({
            "contract_ci_score_level_delivered": delivered,
            "contract_ci_score_level_pending": pending
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

@app.route('/dashboard/plants-ci-score-level', methods=['GET'])
def customer_type_percentage_by_plant():
    try:
        with sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=ACCESS_TOKEN) as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    WITH CustomerTypeBushels AS (
                        SELECT
                            pm.PlantName,
                            SUM(CASE WHEN c.SupplierID = 'C' AND ci.ci_score_final_gc02e_per_bu IS NOT NULL THEN CAST(c.SuppliedQuantity AS DOUBLE) ELSE 0.0 END) AS Grower_Bushels,
                            SUM(CASE WHEN c.SupplierID = 'G' AND ci.ci_score_final_gc02e_per_bu IS NOT NULL THEN CAST(c.SuppliedQuantity AS DOUBLE) ELSE 0.0 END) AS Retailer_Bushels,
                            SUM(CASE WHEN c.SupplierID = 'C' AND ci.ci_score_final_gc02e_per_bu IS NULL THEN CAST(c.SuppliedQuantity AS DOUBLE) ELSE 0.0 END) AS NoScoreGrower_Bushels,
                            SUM(CASE WHEN c.SupplierID = 'G' AND ci.ci_score_final_gc02e_per_bu IS NULL THEN CAST(c.SuppliedQuantity AS DOUBLE) ELSE 0.0 END) AS NoScoreRetailer_Bushels,
                            SUM(CASE WHEN c.SupplierID NOT IN ('C', 'G') THEN CAST(c.SuppliedQuantity AS DOUBLE) ELSE 0.0 END) AS Other_Bushels,
                            SUM(CAST(c.SuppliedQuantity AS DOUBLE)) AS TotalBushels_Plant
                        FROM
                            gold.contractdata c
                        INNER JOIN
                            gold.plant_master pm ON pm.PlantId = c.PlantID
                        LEFT OUTER JOIN
                            bronze.cultura_ci ci ON ci.producer_id = c.NameID
                        GROUP BY
                            pm.PlantName
                    )
                    SELECT
                        PlantName,
                        ROUND((Grower_Bushels * 100.0) / NULLIF(TotalBushels_Plant, 0), 2) AS Grower_Percentage,
                        ROUND((Retailer_Bushels * 100.0) / NULLIF(TotalBushels_Plant, 0), 2) AS Retailer_Percentage,
                        ROUND((NoScoreGrower_Bushels * 100.0) / NULLIF(TotalBushels_Plant, 0), 2) AS NoScoreGrower_Percentage,
                        ROUND((NoScoreRetailer_Bushels * 100.0) / NULLIF(TotalBushels_Plant, 0), 2) AS NoScoreRetailer_Percentage,
                        ROUND((Other_Bushels * 100.0) / NULLIF(TotalBushels_Plant, 0), 2) AS Other_Percentage
                    FROM
                        CustomerTypeBushels
                    ORDER BY
                        PlantName;
                """)

                result = cursor.fetchall()
                response_data = [
                    {
                        "plant_name": row[0],
                        "grower_percentage": row[1],
                        "retailer_percentage": row[2],
                        "no_score_grower_percentage": row[3],
                        "no_score_retailer_percentage": row[4],
                        "other_percentage": row[5]
                    }
                    for row in result if len(row) >= 6
                ]

        return jsonify(response_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500    
    

@app.route('/sourcing/sources', methods=['GET'])
def producer_bushels_with_ci():
    try:
        with sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=ACCESS_TOKEN) as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        p.Name,
                        p.Type,
                        SUM(cq.QtyOfBushels) AS Bushels,
                        (SUM(cq.QtyOfBushels) * 100.0 / SUM(SUM(cq.QtyOfBushels)) OVER ()) AS PercentOfTotal,
                        ci.ci_score_final_gc02e_per_MJ    
                    FROM
                        gold.producer p
                    INNER JOIN
                        gold.contract c ON p.NameID = c.NameID
                    INNER JOIN
                        gold.contractqty cq ON cq.ContractID = c.ContractID
                    INNER JOIN
                        bronze.cultura_ci ci ON ci.producer_id = p.ERPNameID
                    GROUP BY
                        p.Name,
                        p.Type,
                        ci.ci_score_final_gc02e_per_MJ;
                """)

                result = cursor.fetchall()
                response_data = [
                    {
                        "source": row[0],
                        "type": row[1],
                        "bushels": row[2],
                        "percent_of_total": row[3],
                        "ci_score_per_MJ": row[4]
                    }
                    for row in result if len(row) >= 5
                ]

        return jsonify(response_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/sourcing/opportunites-map', methods=['GET'])
def producer_location_ci():
    try:
        with sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=ACCESS_TOKEN) as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        p.Name,
                        p.Type,
                        CASE 
                            WHEN p.Lat IS NULL THEN ci.latitude
                            ELSE p.Lat
                        END AS latitude,
                        CASE 
                            WHEN p.Lon IS NULL THEN ci.longitude
                            ELSE p.Lon
                        END AS longitude,
                        ci.ci_score_final_gc02e_per_MJ
                    FROM
                        gold.producer p
                    INNER JOIN
                        bronze.cultura_ci ci ON ci.producer_id = p.ERPNameID
                """)
                
                rows = cursor.fetchall()

                result = [
                    {
                        "name": row[0],
                        "type": row[1],
                        "latitude": row[2],
                        "longitude": row[3],
                        "ci_score": row[4]
                    }
                    for row in rows if len(row) == 5
                ]

        return jsonify(result)
    
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
                        query = "SELECT * FROM gold.plantinfo WHERE plantid = ?"
                        cursor.execute(query, (plant_id,))
                    else:
                        # print("Received GET request without plantid")
                        query = "SELECT * FROM gold.plantinfo"
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
                "naturalgasrenewable45z", "convefficiency", "fromdate", "todate",  "createdby"
            ]
            for field in required_fields:
                if field not in data:
                    return jsonify({"error": f"Missing field: {field}"}), 400

            with sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=ACCESS_TOKEN) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO gold.plantinfo (
                             plantid, totalbushelsprocessed, totalethanolproduced,
                            gridelectricusage, renewablelectricusage, fossilgasused, coalusage,
                            naturalgasrenewable45z, convefficiency, fromdate, todate,
                             createdby
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,  ?, ?, ?)
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
                        data["fromdate"],
                        data["todate"],
                        # data.get("updatedon"),  # Can be None
                        data["createdby"]
                    ))

            return jsonify({"status": "Manual plant input inserted successfully ✅"})

        except Exception as e:
            return jsonify({"error": str(e)}), 500
        
@app.route('/setting/business-rules', methods=['GET'])
def business_rules_handler():
    try:
        with sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=ACCESS_TOKEN) as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    select p.Name from gold.ciscore as ci inner join gold.producer as p on ci.nameid=p.NameID where p.Type='G'
                """)
                
                rows = cursor.fetchall()  # [(name1,), (name2,), ...]

                # Print formatted for debug like Row(Name='...')
                print([f"Row(Name='{row[0]}')" for row in rows])

                # Return as plain JSON
                return jsonify([{"Name": row[0]} for row in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500
# Run app
if __name__ == '__main__':
    app.run(debug=True, port=3000, host="localhost")
    # app.run(debug=True, port=3000, host="0.0.0.0")
