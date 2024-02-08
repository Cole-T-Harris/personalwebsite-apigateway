from pydantic import BaseModel
from typing import List
import json
import requests
import boto3
import time
# import pgeocode
# import geopy.distance

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
    # thumbnail: str
    hours: Hours
    # distance: float

    # def to_json(self):
    #     """Converts the Store object to a JSON-formatted string."""
    #     return self.json()

    # @classmethod
    # def from_json(cls, json_str):
    #     """Creates a Store object from a JSON-formatted string."""
    #     return cls.parse_raw(json_str)
    
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

# def get_distance(store, zipcode):
#     nomi = pgeocode.Nominatim('us')
#     zipcode_latlong_dataframe = nomi.query_postal_code(zipcode)[['latitude','longitude']]
#     starting_coordinates = (zipcode_latlong_dataframe['latitude'], zipcode_latlong_dataframe['longitude'])
#     store_coordinates = (store["geolocation"]["latitude"], store["geolocation"]["longitude"])
#     distance = geopy.distance.geodesic(starting_coordinates, store_coordinates).miles
#     return distance
    
def get_oauth_header():
    dynamo_response = dynamoDB_table.get_item(
        Key={'TokenKey': TOKEN_KEY}
    )
    print(f"DynamoDB Response: {dynamo_response}")
    if (dynamo_response["ResponseMetadata"]["HTTPStatusCode"] != 200):
        print("Failed to store token in database")
        return {
            'statusCode': 500,
            'body': 'Internal Server Error'
        }
    token_expiration = dynamo_response["Item"]["expiresIn"]
    token = dynamo_response["Item"]["token"]
    current_time = time.time()

    if token_expiration <= current_time:
        lambda_client = boto3.client('lambda')
        lambda_function = "kroger-oauth"
        payload = {
            "tokenKey": TOKEN_KEY,
            "scope": SCOPE
        }
        token_response = lambda_client.invoke(
            FunctionName=lambda_function,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        token_payload = json.loads(token_response['Payload'].read().decode('utf-8'))
        print(f"Oauth Lambda Response: {token_payload}")
        if (token_payload["statusCode"] != 200):
            print("Failed on invoking oauth token lambda")
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
    # for store in stores_response_json['data']:
    #     store["distance"] = get_distance(store=store, zipcode=location_params.zipcode)
        # store["thumbnail"] = get_thumbnail(store["chain"], db)
    print(f"Kroger Locations Request Response: {stores_response_json}")
    locations_response = LocationsResponse(zipcode=zipcode,
                                           radiusInMiles=radiusInMiles,
                                           limit=limit,
                                           stores=stores_response_json['data'])
    return json.dumps(locations_response.to_dict())


