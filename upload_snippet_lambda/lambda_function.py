import json
import pymysql
import boto3
import uuid
from configparser import ConfigParser
from cryptography.fernet import Fernet
import requests

# Load Config
config_file = "upload_config.ini"
config = ConfigParser()
config.read(config_file)

# Database Configuration
DB_HOST = config["rds"]["endpoint"]
DB_USER = config["rds"]["user_name"]
DB_PASSWORD = config["rds"]["user_pwd"]
DB_NAME = config["rds"]["db_name"]
DB_PORT = int(config["rds"]["port_number"])

# S3 Configuration
S3_BUCKET = config["s3"]["bucket_name"]
S3_SNIPPETS_FOLDER = config["s3"]["snippets_folder"]
S3_CLIENT = boto3.client("s3")

# API Gateway Authentication Endpoint
AUTH_API_URL = config["auth"]["api_url"]

# Encryption Key
FERNET_KEY = config["encryption"]["fernet_key"]
cipher = Fernet(FERNET_KEY.encode())

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

# Encrypt Function
def encrypt_snippet(snippet_text):
    return cipher.encrypt(snippet_text.encode()).decode()

# Lambda Handler for Upload
def lambda_handler(event, context):
    try:
        print("** Upload Lambda Triggered **")

        # Validate Token from Authorization header
        if "headers" not in event or "Authorization" not in event["headers"]:
            return {"statusCode": 401, "body": json.dumps({"error": "Missing Authorization token"})}

        auth_header = event["headers"]["Authorization"]
        token = auth_header.split(" ")[1] if " " in auth_header else auth_header  # Support "Bearer <token>" or plain token

        auth_response = requests.post(AUTH_API_URL, json={"token": token})

        if auth_response.status_code != 200:
            return {"statusCode": 401, "body": json.dumps({"error": "Invalid or expired token"})}

        # Extract the authenticated userId
        authenticated_user_id = json.loads(auth_response.text)["userId"]

        # Parse Request
        body = json.loads(event["body"])
        file_name = body.get("fileName")
        file_content = body.get("fileContent")

        if not file_name or not file_content:
            return {"statusCode": 400, "body": json.dumps({"error": "Missing required fields"})}

        # Database Connection
        connection = get_db_connection()

        with connection.cursor() as cursor:
            # Checking if the file already exists for this suer
            cursor.execute("SELECT snippetId FROM Snippets WHERE fileName = %s AND ownerId = %s", 
                           (file_name, authenticated_user_id))
            existing_snippet = cursor.fetchone()

            if existing_snippet:
                return {"statusCode": 400, "body": json.dumps({"error": "A file with this name already exists for your account."})}

            # Generate new snippet ID and S3 key
            snippet_id = str(uuid.uuid4())
            s3_key = f"{S3_SNIPPETS_FOLDER}/{file_name}"
            s3_uri = f"s3://{S3_BUCKET}/{s3_key}"

            # Encrypt Content
            encrypted_data = encrypt_snippet(file_content)

            # Upload to S3
            S3_CLIENT.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=encrypted_data)
            print(f"** Uploaded to S3: {s3_uri} **")

            file_extension = file_name.split(".")[-1]

            # Retrieve the owner's username from the Users table
            cursor.execute("SELECT username FROM Users WHERE userId = %s", (authenticated_user_id,))
            owner_info = cursor.fetchone()
            owner_username = owner_info["username"] if owner_info else "Unknown"  # Ensure a default value if missing

            # Store Metadata in Database with ownerUsername
            sql = """INSERT INTO Snippets (snippetId, ownerId, ownerUsername, fileName, fileType, s3Path, encryptionKey, allowedUsers)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
            cursor.execute(sql, (snippet_id, authenticated_user_id, owner_username, file_name, file_extension, s3_uri, FERNET_KEY, "[]"))

            # Increment the owner's upload count
            cursor.execute("UPDATE Users SET totalUploads = IFNULL(totalUploads, 0) + 1 WHERE userId = %s", (authenticated_user_id,))


        connection.commit()
        connection.close()
        print(f"** Metadata stored in DB for snippet: {snippet_id} **")

        return {
            "statusCode": 201,
            "body": json.dumps({"message": "Upload successful", "snippetId": snippet_id, "s3Uri": s3_uri})
        }

    except pymysql.MySQLError as e:
        error_msg = str(e)
        if "foreign key constraint fails" in error_msg:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "error": "Invalid ownerId",
                    "details": "The ownerId value in Snippets does not exist in the Users table. Please ensure the user exists before inserting a snippet."
                })
            }
        elif "Duplicate entry" in error_msg:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "error": "Duplicate snippetId",
                    "details": "The snippetId already exists in the database. Please use a unique snippetId."
                })
            }
        else:
            return {
                "statusCode": 500,
                "body": json.dumps({
                    "error": "Database error",
                    "details": error_msg
                })
            }
    except Exception as e:
        print(f"** General Error: {str(e)} **")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
