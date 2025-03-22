import json
import pymysql
import boto3
from configparser import ConfigParser
from cryptography.fernet import Fernet
import requests
import datetime

# Load Config
config_file = "update_config.ini"
config = ConfigParser()
config.read(config_file)

# Database Config
DB_HOST = config["rds"]["endpoint"]
DB_USER = config["rds"]["user_name"]
DB_PASSWORD = config["rds"]["user_pwd"]
DB_NAME = config["rds"]["db_name"]
DB_PORT = int(config["rds"]["port_number"])

# S3 Config
S3_BUCKET = config["s3"]["bucket_name"]
S3_CLIENT = boto3.client("s3")

# Auth Config
AUTH_API_URL = config["auth"]["api_url"]

# Encryption
FERNET_KEY = config["encryption"]["fernet_key"]
cipher = Fernet(FERNET_KEY.encode())

def get_db_connection():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=DB_PORT,
        cursorclass=pymysql.cursors.DictCursor
    )

def encrypt_snippet(snippet_text):
    return cipher.encrypt(snippet_text.encode()).decode()

def lambda_handler(event, context):
    connection = None
    try:
        print("** Update Lambda Triggered **")

        # Validate Token
        if "headers" not in event or "Authorization" not in event["headers"]:
            return {"statusCode": 401, "body": json.dumps({"error": "Missing Authorization token"})}

        auth_header = event["headers"]["Authorization"]
        token = auth_header.split(" ")[1] if " " in auth_header else auth_header

        auth_response = requests.post(AUTH_API_URL, json={"token": token})
        if auth_response.status_code != 200:
            return {"statusCode": 401, "body": json.dumps({"error": "Invalid or expired token"})}

        requester_id = json.loads(auth_response.text)["userId"]

        # Parse body
        body = json.loads(event["body"])
        file_name = body.get("fileName")
        new_file_content = body.get("fileContent")

        if not file_name or not new_file_content:
            return {"statusCode": 400, "body": json.dumps({"error": "Missing fileName or fileContent"})}

        connection = get_db_connection()
        with connection.cursor() as cursor:
            # Verify snippet exists and user has permission to update it
            cursor.execute("""
                SELECT snippetId, ownerId, allowedUsers, s3Path, fileName
                FROM Snippets
                WHERE fileName = %s AND (ownerId = %s OR JSON_CONTAINS(allowedUsers, %s))
            """, (file_name, requester_id, json.dumps(requester_id)))

            snippet = cursor.fetchone()

            snippet_id = snippet["snippetId"]

            if not snippet:
                return {"statusCode": 404, "body": json.dumps({"error": "Snippet not found."})}

            allowed_users = json.loads(snippet["allowedUsers"])

            # Check if requester is either the owner or in the allowedUsers list
            if requester_id != snippet["ownerId"] and requester_id not in allowed_users:
                return {"statusCode": 403, "body": json.dumps({"error": "You do not have permission to update this snippet."})}

            old_s3_key = snippet["s3Path"].replace(f"s3://{S3_BUCKET}/", "")
            file_name = snippet["fileName"]

            # Encrypt new content
            encrypted_data = encrypt_snippet(new_file_content)

            # Delete old file from S3
            S3_CLIENT.delete_object(Bucket=S3_BUCKET, Key=old_s3_key)
            print(f"Deleted old file from S3: {old_s3_key}")

            # Upload new encrypted file to S3
            S3_CLIENT.put_object(Bucket=S3_BUCKET, Key=old_s3_key, Body=encrypted_data)
            print(f"Uploaded new version to S3: {old_s3_key}")

            # Update timestamp in DB
            updated_at = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute("""
                UPDATE Snippets
                SET lastUpdated = %s
                WHERE snippetId = %s
            """, (updated_at, snippet_id))

            connection.commit()

            # Trigger Extract Metadata Lambda
            lambda_client = boto3.client("lambda")

            extract_payload = {
                "headers": {
                    "Authorization": f"Bearer {token}"
                },
                "body": json.dumps({
                    "snippetId": snippet_id,
                    "fileName": file_name,
                    "snippetText": new_file_content
                })
            }

            response = lambda_client.invoke(
                FunctionName="project_extract_metadata",
                InvocationType="Event",
                Payload=json.dumps(extract_payload)
            )

            print(f"Triggered Extract Metadata Lambda for snippet: {snippet_id} | Response Code: {response['StatusCode']}")


        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Snippet updated successfully.",
                "snippetId": snippet_id,
                "updatedAt": updated_at
            })
        }

    except pymysql.MySQLError as e:
        return {"statusCode": 500, "body": json.dumps({"error": "Database error", "details": str(e)})}
    except Exception as e:
        print(f"** General Error: {str(e)} **")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
    finally:
        if connection:
            connection.close()
            print("** Database Connection Closed **")
