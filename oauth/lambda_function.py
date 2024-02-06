import requests
import os
import base64
import json

KROGER_TOKEN_URL = "https://api.kroger.com/v1/connect/oauth2/token"

def lambda_handler(event, context):
    print("received event: " + json.dumps(event, indent=2))
    client_id = os.environ.get('KROGER_OAUTH_CLIENT_ID')
    client_secret = os.environ.get("KROGER_OAUTH_CLIENT_SECRET")
    credentials_string = f"{client_id}:{client_secret}"
    encoded_credentials = base64.b64encode(credentials_string.encode("utf-8")).decode("utf-8")
    auth_headers = {
        'Accept': 'application/json; charset=utf-8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {encoded_credentials}"
    }
    auth_params = {
        "grant_type": "client_credentials",
        "scope": event["scope"]
    }
    try:
        response = requests.post(KROGER_TOKEN_URL, data=auth_params, headers=auth_headers)
    except Exception as e:
        print(f"Failed to obtain OAuth token {str(e)}")
        return {
            'statusCode': 500,
            'body': 'Internal Server Error'
        }
    if response.status_code == 200:
        token = response.json()['access_token']
        expires_in = response.json()['expires_in']
    else:
        return None
    return {
        'statusCode': 200,
        'body': {
            'token': token,
            'expiresIn': expires_in
        }
    }


