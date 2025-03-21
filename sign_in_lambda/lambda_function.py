import json
import pymysql
import boto3
import bcrypt
import configparser

# Load configuration
config_file = "sign_in_config.ini"
config = configparser.ConfigParser()
config.read(config_file)

# RDS MySQL Configuration
DB_HOST = config["rds"]["endpoint"]
DB_USER = config["rds"]["user_name"]
DB_PASSWORD = config["rds"]["user_pwd"]
DB_NAME = config["rds"]["db_name"]
DB_PORT = int(config["rds"]["port_number"])

# AWS Lambda Client (to call Auth Lambda)
lambda_client = boto3.client("lambda")

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

# Sign-In Function
def lambda_handler(event, context):
    """
    Authenticates a user by verifying the username and hashed password.
    If valid, calls Auth Lambda to generate a token.
    """
    print("** Sign-In Lambda Triggered **")

    try:
        # Parse request body
        if "body" not in event:
            print("** ERROR: Missing body in request **")
            return {"statusCode": 400, "body": json.dumps({"error": "No body in request"})}

        body = json.loads(event["body"])
        username = body.get("username")
        password = body.get("password")

        print(f"** Username received: {username} **")

        # Validate Inputs
        if not username or not password:
            print("** ERROR: Missing username or password **")
            return {"statusCode": 400, "body": json.dumps({"error": "Missing username or password"})}

        # Connect to database
        connection = get_db_connection()
        print("** Connected to Database **")

        with connection.cursor() as cursor:
            # Fetch user details
            sql = "SELECT userId, passwordHash FROM Users WHERE username = %s"
            cursor.execute(sql, (username,))
            user = cursor.fetchone()

            if not user:
                print("** ERROR: Invalid credentials (User not found) **")
                return {"statusCode": 401, "body": json.dumps({"error": "Invalid credentials"})}

            # Verify password using bcrypt
            stored_hashed_password = user["passwordHash"].encode()
            if not bcrypt.checkpw(password.encode(), stored_hashed_password):
                print("** ERROR: Invalid credentials (Password mismatch) **")
                return {"statusCode": 401, "body": json.dumps({"error": "Invalid credentials"})}

            user_id = user["userId"]

        # Close DB connection
        connection.close()
        print(f"** User {username} authenticated successfully **")

        # Call Auth Lambda to get a token
        auth_payload = {
            "body": json.dumps({"username": username, "password": password, "duration": 30})  # 30 min token
        }
        print("** Invoking Auth Lambda to generate token **")

        auth_response = lambda_client.invoke(
            FunctionName="project_auth",  # Change this to your Auth Lambda name
            InvocationType="RequestResponse",
            Payload=json.dumps(auth_payload),
        )

        # Read and parse the response
        auth_result = json.loads(auth_response["Payload"].read().decode())
        print(f"** Auth Lambda Response: {auth_result} **")

        if auth_result["statusCode"] != 200:
            print("** ERROR: Failed to generate token from Auth Lambda **")
            return {"statusCode": 500, "body": json.dumps({"error": "Failed to generate token"})}

        token_data = json.loads(auth_result["body"])
        token = token_data.get("token")

        if not token:
            print("** ERROR: Token missing from Auth Lambda response **")
            return {"statusCode": 500, "body": json.dumps({"error": "Token generation failed"})}

        print(f"** Token generated successfully for user {username} **")

        return {"statusCode": 200, "body": json.dumps({"userId": user_id, "token": token, "message": "Sign in successful"})}

    except pymysql.MySQLError as e:
        print(f"** DATABASE ERROR: {str(e)} **")
        return {"statusCode": 500, "body": json.dumps({"error": "Database error", "details": str(e)})}

    except Exception as e:
        print(f"** GENERAL ERROR: {str(e)} **")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
