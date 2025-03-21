import json
import pymysql
import requests
from configparser import ConfigParser

# Load Config
config_file = "dashboard_config.ini"
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
        print("** Dashboard Lambda Triggered **")

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
            # Fetch username of the requester
            cursor.execute("SELECT username FROM Users WHERE userId = %s", (requester_id,))
            requester_user = cursor.fetchone()
            requester_username = requester_user["username"] if requester_user else "Unknown"

            # Fetch snippets owned by or shared with the user
            cursor.execute("""
                SELECT fileName, ownerUsername, lastUpdated, allowedUsers
                FROM Snippets
                WHERE ownerUsername = (SELECT username FROM Users WHERE userId = %s)
                OR JSON_CONTAINS(allowedUsers, JSON_QUOTE((SELECT username FROM Users WHERE userId = %s)))
            """, (requester_id, requester_id))

            snippets = cursor.fetchall()

            # Extract user IDs from allowedUsers
            user_ids = set()
            for snippet in snippets:
                if snippet["allowedUsers"]:
                    user_ids.update(json.loads(snippet["allowedUsers"]))

            # Fetch usernames for allowedUsers
            if user_ids:
                cursor.execute("SELECT userId, username FROM Users WHERE userId IN %s", (tuple(user_ids),))
                user_map = {row["userId"]: row["username"] for row in cursor.fetchall()}
            else:
                user_map = {}

            # Format the response
            formatted_snippets = [
                {
                    "fileName": snippet["fileName"],
                    "owner": snippet["ownerUsername"],
                    "lastModified": snippet["lastUpdated"].strftime('%Y-%m-%d %H:%M:%S') if snippet["lastUpdated"] else None,
                    "usersWithAccess": [user_map.get(uid, "Unknown") for uid in json.loads(snippet["allowedUsers"])] if snippet["allowedUsers"] else []
                }
                for snippet in snippets
            ]

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Dashboard retrieved successfully.",
                "account": requester_username,
                "snippets": formatted_snippets
            }, indent=2)
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
