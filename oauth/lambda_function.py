import requests
import os
import base64
import json
import boto3
from datetime import datetime, timedelta

KROGER_TOKEN_URL = "https://api.kroger.com/v1/connect/oauth2/token"
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table_name = 'OauthToken'
dynamoDB_table = dynamodb.Table(table_name)

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
        current_time = datetime.now()
        expiration_time = current_time + timedelta(seconds=expires_in)
        expiration_timestamp = int(expiration_time.timestamp())
        dynamoResponse = dynamoDB_table.put_item(
            Item= {
                'scope': event["scope"],
                'token': token,
                'expiresIn': expiration_timestamp
            }
        )
        print(f"DynamoDB Response: {dynamoResponse}")
        if (dynamoResponse["ResponseMetadata"]["HTTPStatusCode"] != 200):
            print("Failed to store token in database")
            return {
                'statusCode': 500,
                'body': 'Internal Server Error'
            }
        print(f"Item expires at: {expiration_timestamp}")
    else:
        return None
    return {
        'statusCode': 200
    }

def test_lambda_handler():
    event = {"scope": ""}
    result = lambda_handler(event, None)
    print("Result:", result)

if __name__ == "__main__":
    test_lambda_handler()

