import json
import pymysql
import bcrypt
import uuid
from configparser import ConfigParser

# Load database configuration
config_file = "create_account_config.ini"
config = ConfigParser()
config.read(config_file)

DB_HOST = config.get("rds", "endpoint")
DB_USER = config.get("rds", "user_name")
DB_PASSWORD = config.get("rds", "user_pwd")
DB_NAME = config.get("rds", "db_name")
DB_PORT = int(config.get("rds", "port_number"))


# Function to connect to the MySQL database
def get_db_connection():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=DB_PORT,
        cursorclass=pymysql.cursors.DictCursor
    )


# **Lambda Handler**
def lambda_handler(event, context):
    """
    Creates a new user account with a hashed password.
    """
    body = json.loads(event["body"])
    username = body.get("username")
    password = body.get("password")

    # Validate Inputs
    if not username or not password:
        return {"statusCode": 400, "body": json.dumps({"error": "Missing username or password"})}

    connection = None

    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            # Check if username already exists
            cursor.execute("SELECT userId FROM Users WHERE username = %s", (username,))
            existing_user = cursor.fetchone()

            if existing_user:
                return {"statusCode": 400, "body": json.dumps({"error": "Username already taken"})}

            # Generate unique userId
            user_id = str(uuid.uuid4())

            # Hash password securely
            hashed_password = bcrypt.hashpw(password.encode(), bcrypt.gensalt(rounds=12)).decode()

            # Insert new user into the database
            sql = """INSERT INTO Users (userId, username, passwordHash, totalUploads, totalDownloads, createdAt) 
                     VALUES (%s, %s, %s, %s, %s, NOW())"""
            cursor.execute(sql, (user_id, username, hashed_password, 0, 0))
            connection.commit()

    except pymysql.MySQLError as e:
        return {"statusCode": 500, "body": json.dumps({"error": "Database error", "details": str(e)})}

    finally:
        if connection:
            connection.close()

    return {"statusCode": 201, "body": json.dumps({"userId": user_id, "message": "Account created successfully"})}
