import json
import pymysql
import requests
from collections import Counter
from configparser import ConfigParser

# Load Config
config_file = "summary_config.ini"
config = ConfigParser()
config.read(config_file)

# Database Config
DB_HOST = config["rds"]["endpoint"]
DB_USER = config["rds"]["user_name"]
DB_PASSWORD = config["rds"]["user_pwd"]
DB_NAME = config["rds"]["db_name"]
DB_PORT = int(config["rds"]["port_number"])

# Auth Config
AUTH_API_URL = config["auth"]["api_url"]

def get_db_connection():
    """Establishes and returns a database connection."""
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
        print("** Summary Lambda Triggered **")

        # Validate Token
        if "headers" not in event or "Authorization" not in event["headers"]:
            return {"statusCode": 401, "body": json.dumps({"error": "Missing Authorization token"})}

        auth_header = event["headers"]["Authorization"]
        token = auth_header.split(" ")[1] if " " in auth_header else auth_header

        auth_response = requests.post(AUTH_API_URL, json={"token": token})
        if auth_response.status_code != 200:
            return {"statusCode": 401, "body": json.dumps({"error": "Invalid or expired token"})}

        requester_id = json.loads(auth_response.text)["userId"]

        connection = get_db_connection()
        with connection.cursor() as cursor:
            # Fetch user details
            cursor.execute("SELECT username, totalUploads, totalDownloads FROM Users WHERE userId = %s", (requester_id,))
            user_data = cursor.fetchone()

            if not user_data:
                return {"statusCode": 404, "body": json.dumps({"error": "User not found."})}

            username = user_data["username"]
            total_uploads = user_data["totalUploads"]
            total_downloads = user_data["totalDownloads"]

            # Fetch all files uploaded by the user
            cursor.execute("SELECT fileName FROM Snippets WHERE ownerUsername = %s", (username,))
            snippets = cursor.fetchall()

            # Compute file type distribution
            file_extensions = [file["fileName"].split('.')[-1] for file in snippets if '.' in file["fileName"]]
            file_type_counts = Counter(file_extensions)
            most_active_file_types = dict(file_type_counts.most_common(3))  # Top 3 most uploaded file types

        # Prepare Summary
        summary = {
            "username": username,
            "totalUploads": total_uploads,
            "totalDownloads": total_downloads,
            "mostActiveFileTypes": most_active_file_types
        }

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "User summary retrieved successfully.", "summary": summary}, indent=2)
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
