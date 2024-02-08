import json
from pydantic import BaseModel
from typing import List, Optional
import requests
import boto3
import time

BASE_KROGER_URL = "https://api.kroger.com/v1/"
TOKEN_KEY = 'products'
SCOPE = 'product.compact'
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table_name = 'OauthToken'
dynamoDB_table = dynamodb.Table(table_name)

class AisleLocation(BaseModel):
    bayNumber: Optional[str] = ""
    description: Optional[str] = ""
    number: Optional[str] = ""
    numberOfFacings: Optional[str] = ""
    side: Optional[str] = ""
    shelfNumber: Optional[str] = ""
    shelfPositionInBay: Optional[str] = ""

class ProductImages(BaseModel):
    thumbnail: Optional[str] = None
    frontImage: Optional[str] = None
    backImage: Optional[str] = None
    rightImage: Optional[str] = None
    leftImage: Optional[str] = None

class ProductPrice(BaseModel):
    price: Optional[float] = None
    promo: Optional[float] = None

class ProductPagination(BaseModel):
    start: Optional[int] = 0
    limit: Optional[int] = 0
    total: Optional[int] = 0

class ProductMetaData(BaseModel):
    pagination: ProductPagination

class Product(BaseModel):
    productId: str
    aisleLocations: Optional[List[AisleLocation]] = []
    brand: Optional[str] = ""
    countryOfOrigin: Optional[str] = ""
    description: Optional[str] = ""
    stock: Optional[str] = None
    prices: Optional[ProductPrice] = None
    size: Optional[str] = None
    priceSize: Optional[str] = None
    images: Optional[ProductImages] = None

    def to_dict(self):
        return self.dict()


class ProductsResponse(BaseModel):
    term: str
    locationId: int
    start: int
    limit: int
    products: Optional[List[Product]] = []
    meta: ProductMetaData

    def to_dict(self):
        return self.dict()

def get_image_perspective(image):
    return image["perspective"]

def get_index_of_medium_image(images):
    if len(images) == 5:
        return 2
    else:
        for i in range(len(images)):
            if images[i]["size"] == "medium":
                return i
        return 0
    
def get_index_of_thumbnail_image(images):
    if len(images) == 5:
        return 4
    else:
        for i in range(len(images)):
            if images[i]["size"] == "thumbnail":
                return i
        return -1
    
def get_image_url(image, index):
    return image["sizes"][index]["url"]

def build_product_images(image_array):
    thumbnail = ""
    front_image = ""
    back_image = ""
    left_image = ""
    right_image = ""
    for image in image_array:
        if get_image_perspective(image) == "front":
            front_image = get_image_url(image, get_index_of_medium_image(image["sizes"]))
            thumbnail_index = get_index_of_thumbnail_image(image["sizes"])
            thumbnail = get_image_url(image, thumbnail_index) if thumbnail_index >= 0 else ""
        elif get_image_perspective(image) == "back":
            back_image = get_image_url(image, get_index_of_medium_image(image["sizes"]))
        elif get_image_perspective(image) == "right":
            right_image = get_image_url(image, get_index_of_medium_image(image["sizes"]))
        elif get_image_perspective(image) == "left":
            left_image = get_image_url(image, get_index_of_medium_image(image["sizes"]))
    images = ProductImages(thumbnail=thumbnail, frontImage=front_image, backImage=back_image, rightImage=right_image, leftImage=left_image)
    return images

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
    if "queryStringParameters" not in event:
        return {
            'statusCode': 400,
            'body': 'Invalid request. Missing query parameters'
        }
    term = event["queryStringParameters"].get("term")
    location_id = event["queryStringParameters"].get("locationId")
    start = event["queryStringParameters"].get("start")
    limit = event["queryStringParameters"].get("limit")
    if not term or not location_id or "start" not in event["queryStringParameters"] or not limit:
        return {
            'statusCode': 400,
            'body': 'Invalid request. Term, locationId, start, and limit are requered query parameters'
        }
    auth_header = get_oauth_header()
    products_url = BASE_KROGER_URL + f"products?filter.term={term}&filter.locationId={location_id}&filter.start={start}&filter.limit={limit}"
    try:
        response = requests.get(url=products_url, headers=auth_header)
    except Exception as e:
        print(f"Failed to obtain product info {str(e)}")
        return {
            'statusCode': 500,
            'body': 'Internal Server Error'
        }
    products_response_json = response.json()
    if "data" in products_response_json:
        for product in products_response_json["data"]:
            product["images"] = build_product_images(product["images"])
            product["stock"] = product.get("items", [{}])[0].get("inventory", {}).get("stockLevel", "")
            product["size"] = product.get("items", [{}])[0].get("size", "")
            product["priceSize"] = product.get("items", [{}])[0].get("soldBy", "")
            product["prices"] = ProductPrice(price=product.get("items", [{}])[0].get("price", {}).get("regular", 0.00),
                                                promo=product.get("items", [{}])[0].get("price", {}).get("promo", 0.00))
        products_response = ProductsResponse(term=term,
                                             locationId=location_id,
                                             start=start,
                                             limit=limit,
                                             products=products_response_json["data"],
                                             meta=products_response_json["meta"])
        return {
            'statusCode': 200,
            'body': json.dumps(products_response.to_dict()),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': "https://www.coleharris.dev",
                'Access-Control-Allow-Methods': 'GET',
                'Access-Control-Allow-Headers': 'Content-Type',
            }
        }
    
    print(f"Invalid products info from kroger {products_response_json}")
    return {
        'statusCode': 500,
        'body': 'Internal Server Error'
    }
