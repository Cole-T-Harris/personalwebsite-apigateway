from pydantic import BaseModel
from typing import List, Optional
import json
import requests
import boto3
import time
import geopy.distance

BASE_KROGER_URL = "https://api.kroger.com/v1/"
SCOPE = ''
TOKEN_KEY = 'locations'
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table_name = 'OauthToken'
dynamoDB_table = dynamodb.Table(table_name)

class LocationsBase(BaseModel):
    zipcode: int
    radiusInMiles: int
    limit: int

class Address(BaseModel):
    addressLine1: str
    city: str
    state: str
    zipCode: str
    county: str

class Geolocation(BaseModel):
    latitude: float
    longitude: float

class DailyHours(BaseModel):
    open: str
    close: str

class Hours(BaseModel):
    monday: DailyHours
    tuesday: DailyHours
    wednesday: DailyHours
    thursday: DailyHours
    friday: DailyHours
    saturday: DailyHours
    sunday: DailyHours

class Store(BaseModel):
    locationId: str
    chain: str
    name: str
    address: Address
    geolocation: Geolocation
    # thumbnail: Optional[str]
    hours: Hours
    # distance: Optional[float]
    
    def to_dict(self):
        return self.dict()

class LocationsResponse(BaseModel):
    zipcode: int
    radiusInMiles: int
    limit: int
    stores: List[Store]

    # def to_json(self):
    #     """Converts the LocationsResponse object to a JSON-formatted string."""
    #     return self.json()

    # @classmethod
    # def from_json(cls, json_str):
    #     """Creates a LocationsResponse object from a JSON-formatted string."""
    #     return cls.parse_raw(json_str)
    
    def to_dict(self):
        return self.dict()

def get_distance(store, zipcode_lat, zipcode_long):
    starting_coordinates = (zipcode_lat, zipcode_long)
    store_coordinates = (store["geolocation"]["latitude"], store["geolocation"]["longitude"])
    distance = geopy.distance.geodesic(starting_coordinates, store_coordinates).miles
    return distance

def make_lambda_request(lambda_name, payload):
    lambda_client = boto3.client('lambda')
    response = lambda_client.invoke(
        FunctionName=lambda_name,
        InvocationType='RequestResponse',
        Payload=json.dumps(payload)
    )
    response_payload = json.loads(response['Payload'].read().decode('utf-8'))
    return response_payload
    
def get_oauth_header():
    dynamo_response = dynamoDB_table.get_item(
        Key={'TokenKey': TOKEN_KEY}
    )
    if (dynamo_response["ResponseMetadata"]["HTTPStatusCode"] != 200):
        print("Failed to store token in database")
        print(f"DynamoDB Response: {dynamo_response}")
        return {
            'statusCode': 500,
            'body': 'Internal Server Error'
        }
    token_expiration = dynamo_response["Item"]["expiresIn"]
    token = dynamo_response["Item"]["token"]
    current_time = time.time()

    if token_expiration <= current_time:
        payload = {
            "tokenKey": TOKEN_KEY,
            "scope": SCOPE
        }
        token_payload = make_lambda_request("kroger-oauth", payload)
        if ("statusCode" not in token_payload or token_payload["statusCode"] != 200):
            print("Failed on invoking oauth token lambda")
            print(f"Oauth Lambda Response: {token_payload}")
            return {
                'statusCode': 500,
                'body': 'Internal Server Error'
            }
        token = token_payload["token"]

    return {'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive', 
            'Authorization': f'Bearer {token}',
            'Cache-Control': 'no-chache'}

def lambda_handler(event, context):
    print("received event: " + json.dumps(event, indent=2))
    zipcode = event["zipcode"]
    radiusInMiles = event["radiusInMiles"]
    limit = event["limit"]
    auth_header = get_oauth_header()
    locations_url = BASE_KROGER_URL + f"locations?filter.zipCode.near={zipcode}&filter.radiusInMiles={radiusInMiles}&filter.limit={limit}"
    try:
        response = requests.get(url=locations_url, headers=auth_header)
    except Exception as e:
        print(f"Failed to obtain OAuth token {str(e)}")
        return {
            'statusCode': 500,
            'body': 'Internal Server Error'
        }
    stores_response_json = response.json()
    
    payload = {
        "zipcode": str(zipcode)
    }
    zipcode_payload = make_lambda_request("zipcode-latlongcoords", payload)
    print(f"Zipcode response body: {zipcode_body}")
    for store in stores_response_json['data']:
        if "statusCode" in zipcode_payload and zipcode_payload["statusCode"] == 200:
            store["distance"] = get_distance(store=store, 
                                             zipcode_lat=zipcode_payload["body"]["latitude"],
                                             zipcode_long= zipcode_payload["body"]["longitude"])
        # store["thumbnail"] = get_thumbnail(store["chain"], db)
    locations_response = LocationsResponse(zipcode=zipcode,
                                           radiusInMiles=radiusInMiles,
                                           limit=limit,
                                           stores=stores_response_json['data'])
    return json.dumps(locations_response.to_dict())


