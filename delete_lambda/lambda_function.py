import json
import pymysql
import boto3
from configparser import ConfigParser
import requests

# Load Config
config_file = "delete_config.ini"
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

def get_db_connection():
    """Establish database connection."""
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=DB_PORT,
        cursorclass=pymysql.cursors.DictCursor
    )

def lambda_handler(event, context):
    connection = None
    try:
        print("** Delete Lambda Triggered **")

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

        if not file_name:
            return {"statusCode": 400, "body": json.dumps({"error": "Missing fileName"})}

        connection = get_db_connection()
        with connection.cursor() as cursor:
            # Find the snippet by fileName
            cursor.execute("SELECT snippetId, ownerId, s3Path FROM Snippets WHERE fileName = %s", (file_name,))
            snippet = cursor.fetchone()

            if not snippet:
                return {"statusCode": 404, "body": json.dumps({"error": "Snippet not found."})}

            # Check if requester is the owner
            if requester_id != snippet["ownerId"]:
                return {"statusCode": 403, "body": json.dumps({"error": "You cannot delete a snippet you don't own. You can only edit it!"})}

            s3_key = snippet["s3Path"].replace(f"s3://{S3_BUCKET}/", "")

            # Delete file from S3
            S3_CLIENT.delete_object(Bucket=S3_BUCKET, Key=s3_key)
            print(f"Deleted snippet from S3: {s3_key}")

            # Remove snippet record from database
            cursor.execute("DELETE FROM Snippets WHERE snippetId = %s", (snippet["snippetId"],))

            # Remove snippet metadata from SnippetMetadata table
            cursor.execute("DELETE FROM SnippetMetadata WHERE snippetId = %s", (snippet["snippetId"],))
            print(f"Deleted snippet metadata for snippetId: {snippet['snippetId']}")

            # Update owner's upload count
            cursor.execute("UPDATE Users SET totalUploads = GREATEST(IFNULL(totalUploads, 0) - 1, 0) WHERE userId = %s", (requester_id,))

            # Commit changes
            connection.commit()

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Snippet deleted successfully.",
                "fileName": file_name
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
