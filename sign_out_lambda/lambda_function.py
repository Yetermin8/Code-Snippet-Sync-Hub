import json
import pymysql
from configparser import ConfigParser

# Load configuration
config_file = "sign_out_config.ini"
config = ConfigParser()
config.read(config_file)

# RDS MySQL Configuration
DB_HOST = config.get("rds", "endpoint")
DB_USER = config.get("rds", "user_name")
DB_PASSWORD = config.get("rds", "user_pwd")
DB_NAME = config.get("rds", "db_name")
DB_PORT = int(config.get("rds", "port_number"))

# Establish MySQL Connection
def get_db_connection():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=DB_PORT,
        cursorclass=pymysql.cursors.DictCursor
    )

# Sign-Out Function
def lambda_handler(event, context):
    """
    Logs out a user by deleting their authentication token from the database.
    """
    try:
        print("** Sign-Out Lambda Triggered **")

        # Parse request body
        if "body" not in event:
            print("** ERROR: No body in request **")
            return {"statusCode": 400, "body": json.dumps({"error": "No body in request"})}

        body = json.loads(event["body"])
        token = body.get("token")

        # Validate Inputs
        if not token:
            print("** ERROR: Missing token **")
            return {"statusCode": 400, "body": json.dumps({"error": "Missing token in request"})}

        # Connect to database
        connection = get_db_connection()
        print("** Connected to Database **")

        with connection.cursor() as cursor:
            # Check if token exists
            sql = "SELECT userId FROM Tokens WHERE token = %s"
            cursor.execute(sql, (token,))
            user = cursor.fetchone()

            if not user:
                print("** ERROR: Invalid or expired token **")
                return {"statusCode": 401, "body": json.dumps({"error": "Invalid or expired token"})}

            # Delete token from database
            delete_sql = "DELETE FROM Tokens WHERE token = %s"
            cursor.execute(delete_sql, (token,))
            connection.commit()

            print(f"** Token deleted successfully for user {user['userId']} **")

        # Close DB connection
        connection.close()
        print("** Database Connection Closed **")

        return {"statusCode": 200, "body": json.dumps({"message": "Sign out successful"})}

    except pymysql.MySQLError as e:
        print("** ERROR: Database error **", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": "Database error", "details": str(e)})}

    except Exception as e:
        print("** ERROR: General error **", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
