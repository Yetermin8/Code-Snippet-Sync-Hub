import json
import pymysql
from configparser import ConfigParser
import requests

# Load Config
config_file = "set_permissions_config.ini"
config = ConfigParser()
config.read(config_file)

# Database Configuration
DB_HOST = config["rds"]["endpoint"]
DB_USER = config["rds"]["user_name"]
DB_PASSWORD = config["rds"]["user_pwd"]
DB_NAME = config["rds"]["db_name"]
DB_PORT = int(config["rds"]["port_number"])

# API Gateway Authentication Endpoint
AUTH_API_URL = config["auth"]["api_url"]

# Function to Connect to MySQL
def get_db_connection():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=DB_PORT,
        cursorclass=pymysql.cursors.DictCursor
    )

# Lambda Handler for Setting Permissions
def lambda_handler(event, context):
    connection = None
    try:
        print("** Set Permissions Lambda Triggered **")

        # Validate Token from Authorization header
        if "headers" not in event or "Authorization" not in event["headers"]:
            print("** ERROR: Missing Authorization header **")
            return {"statusCode": 401, "body": json.dumps({"error": "Missing Authorization token"})}

        auth_header = event["headers"]["Authorization"]
        token = auth_header.split(" ")[1] if auth_header.startswith("Bearer ") else auth_header

        print(f"** Token received for validation: {token[:6]}... (masked) **")  # Masking for security

        auth_response = requests.post(AUTH_API_URL, json={"token": token})
        auth_data = auth_response.json()

        if auth_response.status_code != 200:
            response_body = {"error": "Invalid or expired token"}
            if auth_data.get("loggedOut"):
                response_body["loggedOut"] = True
                response_body["error"] = "Session expired. Please log in again."
            print(f"** ERROR: Authentication failed - {response_body} **")
            return {"statusCode": 401, "body": json.dumps(response_body)}

        owner_id = auth_data["userId"]
        print(f"** Authenticated User ID: {owner_id} **")

        if "body" not in event:
            print("** ERROR: Missing request body **")
            return {"statusCode": 400, "body": json.dumps({"error": "Missing request body"})}

        # arse Request
        try:
            body = json.loads(event["body"])
        except json.JSONDecodeError:
            print("** ERROR: Invalid JSON body **")
            return {"statusCode": 400, "body": json.dumps({"error": "Invalid JSON format"})}

        file_name = body.get("fileName")  # User provides fileName instead of snippetId
        target_username = body.get("targetUsername")
        permission_action = body.get("permissionAction")  # "grant" or "revoke"

        if not file_name or not target_username or permission_action not in ["grant", "revoke"]:
            print(f"** ERROR: Missing required fields: fileName={file_name}, targetUsername={target_username}, permissionAction={permission_action} **")
            return {"statusCode": 400, "body": json.dumps({"error": "Missing or invalid required fields."})}

        connection = get_db_connection()

        with connection.cursor() as cursor:
            # Retrieve the snippetId based on filename & ownerId
            cursor.execute("SELECT snippetId, allowedUsers FROM Snippets WHERE s3Path LIKE %s AND ownerId = %s", 
                           (f"%/{file_name}", owner_id))
            snippet = cursor.fetchone()

            if not snippet:
                print(f"** ERROR: User {owner_id} does not own snippet {file_name} or it does not exist **")
                return {"statusCode": 403, "body": json.dumps({"error": "Access denied: You do not own this snippet or it does not exist."})}

            snippet_id = snippet["snippetId"]
            allowed_users = json.loads(snippet["allowedUsers"] or "[]")

            # Check if the target user exists
            cursor.execute("SELECT userId FROM Users WHERE username = %s", (target_username,))
            target_user = cursor.fetchone()

            if not target_user:
                print(f"** ERROR: Target user {target_username} not found **")
                return {"statusCode": 404, "body": json.dumps({"error": "Target user not found."})}

            target_user_id = target_user["userId"]

            if permission_action == "grant":
                if target_user_id not in allowed_users:
                    allowed_users.append(target_user_id)
                    cursor.execute(
                        "UPDATE Snippets SET allowedUsers = %s WHERE snippetId = %s",
                        (json.dumps(allowed_users), snippet_id)
                    )
                    connection.commit()
                    print(f"** Access granted: {target_username} can now access {file_name} **")
                return {"statusCode": 200, "body": json.dumps({"message": f"User '{target_username}' has been granted access to '{file_name}'."})}

            elif permission_action == "revoke":
                if target_user_id in allowed_users:
                    allowed_users.remove(target_user_id)
                    cursor.execute(
                        "UPDATE Snippets SET allowedUsers = %s WHERE snippetId = %s",
                        (json.dumps(allowed_users), snippet_id)
                    )
                    connection.commit()
                    print(f"** Access revoked: {target_username} can no longer access {file_name} **")
                return {"statusCode": 200, "body": json.dumps({"message": f"User '{target_username}' has been revoked access to '{file_name}'."})}

    except pymysql.MySQLError as e:
        print(f"** Database Error: {str(e)} **")
        return {"statusCode": 500, "body": json.dumps({"error": "Database error", "details": str(e)})}

    except Exception as e:
        print(f"** General Error: {str(e)} **")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

    finally:
        if connection:
            connection.close()
            print("** Database Connection Closed **")
