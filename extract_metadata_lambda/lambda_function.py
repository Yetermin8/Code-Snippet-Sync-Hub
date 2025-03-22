import json
import pymysql
import requests
import boto3
from configparser import ConfigParser
import datetime

# Load Config
config_file = "extract_metadata_config.ini"
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

# Amazon Comprehend Client
comprehend = boto3.client("comprehend", region_name=config["aws"]["region"])

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

def extract_metadata(snippet_text, file_name):
    """Extract metadata from the snippet using Amazon Comprehend and file heuristics."""
    print(f"Extracting metadata for: {file_name}")

    file_type = file_name.split(".")[-1].lower()  # Extract file extension
    
    # Extract key phrases using AWS Comprehend
    key_phrases_response = comprehend.detect_key_phrases(
        Text=snippet_text,
        LanguageCode="en"
    )
    key_phrases = [kp["Text"] for kp in key_phrases_response["KeyPhrases"]]

    # Extract named entities (e.g., function names, class names, libraries)
    entities_response = comprehend.detect_entities(
        Text=snippet_text,
        LanguageCode="en"
    )
    entities = [ent["Text"] for ent in entities_response["Entities"]]

    return file_type, key_phrases, entities

def lambda_handler(event, context):
    connection = None
    try:
        print("** Extract Metadata Lambda Triggered **")

        # Validate Token
        if "headers" not in event or "Authorization" not in event["headers"]:
            return {"statusCode": 401, "body": json.dumps({"error": "Missing Authorization token"})}

        auth_header = event["headers"]["Authorization"]
        token = auth_header.split(" ")[1] if " " in auth_header else auth_header

        auth_response = requests.post(AUTH_API_URL, json={"token": token})
        if auth_response.status_code != 200:
            return {"statusCode": 401, "body": json.dumps({"error": "Invalid or expired token"})}

        # Extract userId from authentication response
        authenticated_user_id = json.loads(auth_response.text)["userId"]

        # Parse event body
        if isinstance(event["body"], str):
            body = json.loads(event["body"]) 
        else:
            body = event["body"]

        print("Received event:")
        print(json.dumps(event, indent=2))


        snippet_id = body.get("snippetId")
        file_name = body.get("fileName")
        snippet_text = body.get("snippetText")  # Decrypted snippet content

        if not snippet_id or not file_name or not snippet_text:
            return {"statusCode": 400, "body": json.dumps({"error": "Missing required fields"})}

        # Extract metadata
        file_type, key_phrases, entities = extract_metadata(snippet_text, file_name)

        connection = get_db_connection()
        with connection.cursor() as cursor:
            # Insert metadata into SnippetMetadata

            print(f"Inserting metadata for snippet: {snippet_id}")
            sql = """
                INSERT INTO SnippetMetadata (snippetId, fileType, keyPhrases, entities, lastUpdated, popularity, fileName)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    fileType = VALUES(fileType),
                    keyPhrases = VALUES(keyPhrases),
                    entities = VALUES(entities),
                    lastUpdated = VALUES(lastUpdated),
                    fileName = VALUES(fileName)
            """

            
            cursor.execute("DELETE FROM SnippetMetadata WHERE snippetId = %s", (snippet_id,))

            cursor.execute(sql, (
                snippet_id,
                file_type,
                json.dumps(key_phrases),
                json.dumps(entities),
                datetime.datetime.utcnow(),
                0,  # Initial popularity score
                file_name
            ))

        connection.commit()
        print(f"** Metadata stored for snippet: {snippet_id} **")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Metadata extracted and stored successfully.",
                "snippetId": snippet_id,
                "fileType": file_type,
                "keyPhrases": key_phrases,
                "entities": entities
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
