import os
import requests
import boto3
from datetime import datetime
import countries_lat_lon

CLIMATE_DATA_BUCKET = 'climate.ml'

appid = os.environ['openweather_key']
bbox = countries_lat_lon.bbox

bbox_iran = bbox.get('region_a')

baseurl = 'http://api.openweathermap.org/data/2.5/'
yql_url = baseurl + 'box/city?bbox=' + bbox_iran
yql_url += '&units=metric' + '&appid=' + appid + "&format=json"

s3_client = boto3.client('s3')


def put_data_in_s3(data, filename):
    s3_client.put_object(Body=data, Bucket=CLIMATE_DATA_BUCKET, Key=filename)


def main(event, context):

    r = requests.get(yql_url)

    if r.status_code == 200:
        content = r.content
        t = datetime.now()
        filename = 'iran_' + t.strftime('%Y-%m-%dT%H-%M-%S') + '.json'
        put_data_in_s3(content, filename)
