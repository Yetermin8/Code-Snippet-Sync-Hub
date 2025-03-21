import json
import os
import pymysql
from configparser import ConfigParser
import bcrypt
import uuid
import datetime

def hash_password(password):
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def check_password(password, hashed):
    return bcrypt.checkpw(password.encode(), hashed.encode())

def generate_token():
    return str(uuid.uuid4())

def validate_token(dbConn, token):
    """
    Validates an authentication token and checks if it has expired.
    """
    try:
        with dbConn.cursor() as cursor:
            sql = "SELECT userId, expiration_utc FROM Tokens WHERE token = %s"
            cursor.execute(sql, (token,))
            user = cursor.fetchone()

            if not user:
                response = {"statusCode": 401, "body": json.dumps({"error": "Invalid or expired token", "loggedOut": True})}
                print("Auth Lambda Response:", response)
                return response

            # Check if the token is expired
            import datetime
            current_time = datetime.datetime.utcnow()
            expiration_time = user["expiration_utc"]

            if expiration_time < current_time:
                response = {"statusCode": 401, "body": json.dumps({"error": "Session expired. Please log in again.", "loggedOut": True})}
                print("Auth Lambda Response:", response)
                return response

            response = {"statusCode": 200, "body": json.dumps({"userId": user["userId"]})}
            print("Auth Lambda Response:", response)
            return response
    except Exception as e:
        print("**ERROR in Token Validation**", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

def authenticate_user(dbConn, username, password, duration):
    """
    Authenticates user and returns a token.
    """
    try:
        with dbConn.cursor() as cursor:
            sql = "SELECT userId, passwordHash FROM Users WHERE username = %s"
            cursor.execute(sql, (username,))
            user = cursor.fetchone()

            if not user:
                response = {"statusCode": 401, "body": json.dumps({"error": "Invalid credentials"})}
                print("Auth Lambda Response:", response)
                return response

            # Check password
            stored_hashed_password = user["passwordHash"].encode()
            if not bcrypt.checkpw(password.encode(), stored_hashed_password):
                response = {"statusCode": 401, "body": json.dumps({"error": "Invalid credentials"})}
                print("Auth Lambda Response:", response)
                return response

            # Generate Token
            token = generate_token()
            expiration_utc = datetime.datetime.utcnow() + datetime.timedelta(minutes=duration)

            # Store token in database
            sql = "INSERT INTO Tokens (token, userId, expiration_utc) VALUES (%s, %s, %s)"
            cursor.execute(sql, (token, user["userId"], expiration_utc))
            dbConn.commit()

            response = {"statusCode": 200, "body": json.dumps({"token": token})}
            print("Auth Lambda Response:", response)
            return response
    except Exception as e:
        print("**ERROR in Auth Lambda**", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

def lambda_handler(event, context):
    dbConn = None
    try:
        print("** Lambda Auth Handler **")
        
        # Load configuration
        config_file = 'auth_config.ini'
        configur = ConfigParser()
        configur.read(config_file)
        
        # Configure RDS connection
        dbConn = pymysql.connect(
            host=configur.get('rds', 'endpoint'),
            user=configur.get('rds', 'user_name'),
            password=configur.get('rds', 'user_pwd'),
            database=configur.get('rds', 'db_name'),
            port=int(configur.get('rds', 'port_number')),
            cursorclass=pymysql.cursors.DictCursor
        )

        # Parse request body
        if "body" not in event:
            print("** ERROR: No body in request **")
            return {"statusCode": 400, "body": json.dumps({"error": "No body in request"})}
        
        body = json.loads(event["body"])
        
        if "token" in body:
            print("** Validating Token **")
            return validate_token(dbConn, body["token"])
        elif "username" in body and "password" in body:
            print(f"** Authenticating User: {body['username']} **")
            duration = body.get("duration", 30)  # Default to 30 minutes
            return authenticate_user(dbConn, body["username"], body["password"], duration)
        else:
            print("** ERROR: Missing credentials in request **")
            return {"statusCode": 400, "body": json.dumps({"error": "Missing credentials in request"})}

    except Exception as err:
        print("** ERROR **", str(err))
        return {"statusCode": 500, "body": json.dumps({"error": str(err)})}

    finally:
        if dbConn:
            dbConn.close()
            print("** Database Connection Closed **")
