import requests
import json
import sys
from configparser import ConfigParser

# Load Config
config = ConfigParser()
config.read("api_config.ini")

BASE_URL = config["api"]["base_url"]

# =============================== UTILITY FUNCTIONS ===============================
def get_headers(token=None, require_auth=True):
    """Return headers for API requests, including Authorization if required."""
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    elif require_auth:
        print("Warning: No token provided in request headers!")
    return headers

# =============================== API FUNCTIONS ===============================
def create_account():
    """Send a request to create a new account."""
    username = input("Enter username: ").strip()
    password = input("Enter password: ").strip()
    
    payload = {"username": username, "password": password}
    url = f"{BASE_URL}{config['auth']['create_account']}"

    response = requests.post(url, json=payload, headers=get_headers(require_auth=False))
    print(response.json())

def sign_in():
    """Authenticate user and return the token."""
    username = input("Enter username: ").strip()
    password = input("Enter password: ").strip()
    
    payload = {"username": username, "password": password}
    url = f"{BASE_URL}{config['auth']['sign_in']}"

    response = requests.post(url, json=payload, headers=get_headers(require_auth=False))
    data = response.json()

    if response.status_code == 200:
        print(f"Sign in successful. Token: {data['token']}")  # Debugging Token
        return data["token"]
    else:
        print(f"Sign in failed: {data.get('error', 'Unknown error')}")
        return None

def upload_snippet(token):
    """Upload a new snippet to the server."""
    file_name = input("Enter file name: ").strip()
    file_content = input("Enter snippet content:\n")

    payload = {"fileName": file_name, "fileContent": file_content}
    url = f"{BASE_URL}{config['snippets']['upload']}"
    headers = get_headers(token)

    print(f"Uploading snippet {file_name} with headers: {headers}")

    response = requests.post(url, json=payload, headers=headers)
    print(response.json())

def download_snippet(token):
    """Download a snippet from the server."""
    file_name = input("Enter the file name you want to download: ").strip()

    payload = {"fileName": file_name}
    url = f"{BASE_URL}{config['snippets']['download']}"

    response = requests.post(url, json=payload, headers=get_headers(token))
    data = response.json()

    if response.status_code == 200:
        print("Snippet downloaded successfully.")
        print("\n=== Snippet Content ===\n")
        print(data["content"])
    else:
        print(f"Download failed: {data.get('error', 'Unknown error')}")

def update_snippet(token):
    """Update an existing snippet."""
    snippet_id = input("Enter snippet ID: ").strip()
    new_content = input("Enter new snippet content:\n")

    payload = {"snippetId": snippet_id, "fileContent": new_content}
    url = f"{BASE_URL}{config['snippets']['update']}"

    response = requests.put(url, json=payload, headers=get_headers(token))
    print(response.json())

def set_permissions(token):
    """Grant or revoke permissions for another user."""
    file_name = input("Enter file name: ").strip()
    target_username = input("Enter target username: ").strip()
    action = input("Enter action (grant/revoke): ").strip().lower()

    if action not in ["grant", "revoke"]:
        print("Invalid action. Use 'grant' or 'revoke'.")
        return

    payload = {"fileName": file_name, "targetUsername": target_username, "permissionAction": action}
    url = f"{BASE_URL}{config['snippets']['set_permissions']}"

    response = requests.post(url, json=payload, headers=get_headers(token))
    print(response.json())

def project_summary(token):
    """Fetch user summary including total uploads, downloads, and most active file types."""
    url = f"{BASE_URL}/summary"
    headers = get_headers(token)
    
    print("Fetching user summary...")
    response = requests.get(url, headers=headers)
    
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text}")

    print(response.json())

def delete_snippet(token):
    """Delete a snippet owned by the user."""
    file_name = input("Enter file name to delete: ").strip()
    payload = {"fileName": file_name}
    url = f"{BASE_URL}/delete"
    
    print(f"Attempting to delete {file_name}...")
    response = requests.delete(url, json=payload, headers=get_headers(token))
    
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text}")

    print(response.json())

def view_dashboard(token):
    """View all files the user has access to, including owner and last modified date."""
    url = f"{BASE_URL}/dashboard"
    
    print("Fetching dashboard...")
    response = requests.get(url, headers=get_headers(token))
    
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text}")

    print(response.json())

def sign_out(token):
    """Sign out the user by invalidating their session."""
    url = f"{BASE_URL}/sign-out"
    
    print("Signing out...")
    response = requests.post(url, headers=get_headers(token))
    
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text}")

    print(response.json())

    return None if response.status_code == 200 else token


# =============================== MAIN MENU ===============================

def prompt():
    """Display menu options and get user input."""
    print("\n** Code Snippet Sync Hub **")
    print("1. Create Account")
    print("2. Sign In")
    print("3. Upload Snippet")
    print("4. Download Snippet")
    print("5. Update Snippet")
    print("6. Set Permissions")
    print("7. View Summary")
    print("8. Delete Snippet")
    print("9. View Dashboard")
    print("10. Sign Out")
    print("0. Exit")
    try:
        return int(input("Enter command: "))
    except ValueError:
        return -1

if __name__ == "__main__":
    print("** Welcome to Code Snippet Sync Hub **")
    token = None  # User's authentication token

    while True:
        cmd = prompt()
        if cmd == 1:
            create_account()
        elif cmd == 2:
            token = sign_in()
        elif cmd == 3 and token:
            upload_snippet(token)
        elif cmd == 4 and token:
            download_snippet(token)
        elif cmd == 5 and token:
            update_snippet(token)
        elif cmd == 6 and token:
            set_permissions(token)
        elif cmd == 7 and token:
            project_summary(token)
        elif cmd == 8 and token:
            delete_snippet(token)
        elif cmd == 9 and token:
            view_dashboard(token)
        elif cmd == 10 and token:
            token = sign_out(token)
        elif cmd == 0:
            sys.exit(0)
        else:
            print("Invalid command or authentication required.")
