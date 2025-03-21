import json
import pymysql
import boto3
from configparser import ConfigParser
from cryptography.fernet import Fernet
import requests

# Load Config
config_file = "download_config.ini"
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
    """Establish a database connection."""
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=DB_PORT,
        cursorclass=pymysql.cursors.DictCursor
    )

def decrypt_snippet(ciphertext):
    """Decrypts a given snippet."""
    return cipher.decrypt(ciphertext.encode()).decode()

def lambda_handler(event, context):
    connection = None
    try:
        print("** Download Lambda Triggered **")

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
        requested_filename = body.get("fileName")
        if not requested_filename:
            return {"statusCode": 400, "body": json.dumps({"error": "Missing fileName"})}

        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT snippetId, s3Path, ownerId, allowedUsers
                FROM Snippets
                WHERE fileName = %s AND (JSON_CONTAINS(allowedUsers, %s) OR ownerId = %s)
            """, (requested_filename, json.dumps(requester_id), requester_id))

            snippet = cursor.fetchone()
            if not snippet:
                return {"statusCode": 403, "body": json.dumps({"error": "Access denied or file not found."})}

            # Prevent re-downloading the same file from the same owner
            cursor.execute("""
                SELECT COUNT(*) as count
                FROM Downloads
                WHERE userId = %s AND fileName = %s AND snippetOwnerId = %s
            """, (requester_id, requested_filename, snippet["ownerId"]))
            prior_download = cursor.fetchone()
            if prior_download["count"] > 0:
                return {"statusCode": 409, "body": json.dumps({"error": "You have already downloaded this file from this user."})}

            # Fetch and decrypt snippet from S3
            s3_key = snippet["s3Path"].replace(f"s3://{S3_BUCKET}/", "")
            s3_response = S3_CLIENT.get_object(Bucket=S3_BUCKET, Key=s3_key)
            encrypted_content = s3_response["Body"].read().decode()
            decrypted_content = decrypt_snippet(encrypted_content)

            # Fetch owner's username
            cursor.execute("SELECT username FROM Users WHERE userId = %s", (snippet["ownerId"],))
            owner_info = cursor.fetchone()
            owner_username = owner_info["username"] if owner_info else "Unknown"

            # Update download counts
            cursor.execute("UPDATE Users SET totalDownloads = totalDownloads + 1 WHERE userId = %s", (requester_id,))
            cursor.execute("UPDATE Snippets SET downloadCount = IFNULL(downloadCount, 0) + 1 WHERE snippetId = %s", (snippet["snippetId"],))
            
            # Log download with owner's username
            cursor.execute("""
                INSERT INTO Downloads (userId, snippetId, snippetOwnerId, ownerUsername, fileName)
                VALUES (%s, %s, %s, %s, %s)
            """, (requester_id, snippet["snippetId"], snippet["ownerId"], owner_username, requested_filename))

            connection.commit()

        print(f"Snippet Content:\n{decrypted_content}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Snippet download successful.",
                "snippetId": snippet["snippetId"],
                "content": decrypted_content
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
