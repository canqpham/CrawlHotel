import json
import os
import time
import logging
from datetime import datetime
from hashlib import md5
from kafka import KafkaConsumer
import requests
import sys
import traceback
import re
import random
import pymongo
from bson.objectid import ObjectId

logging.basicConfig(level=logging.INFO)

KAFKA_HOST = os.environ.get('KAFKA_HOST')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC')
KAFKA_CONSUMER_GROUP = os.environ.get('KAFKA_CONSUMER_GROUP')
MONGO_CONNECTION_STRING = os.environ.get('MONGO_CONNECTION_STRING')
MONGO_DB_NAME = os.environ.get('MONGO_DB_NAME')

consumer = KafkaConsumer(group_id=f"{KAFKA_CONSUMER_GROUP}", bootstrap_servers=KAFKA_HOST)
consumer.subscribe([f"{KAFKA_TOPIC}"])

sizeGet = 1000

findHotelApiHost = "https://4uygjp42kq-dsn.algolia.net/1/indexes/prod_hotel_v2/query?x-algolia-agent=Algolia%20for%20vanilla%20JavaScript%203.32.0&x-algolia-application-id=4UYGJP42KQ&x-algolia-api-key=efa703d5c0057a24487bc9bdcb597770&x-algolia-usertoken=d00c47a3-6590-4ed2-873b-97e873e205b8"
def generateQuery(lat, lng):
    return {
        "params": f"page=0&hitsPerPage={sizeGet}&facets=%5B%22*%22%5D&filters=isDeleted%20%3D%200&facetFilters=%5B%5D&numericFilters=%5B%5D&optionalFilters=%5B%5D&attributesToRetrieve=%5B%22_geoloc%22%2C%22chainId%22%2C%22checkInTime%22%2C%22checkOutTime%22%2C%22facilities%22%2C%22guestRating%22%2C%22guestType%22%2C%22imageURIs%22%2C%22popularity%22%2C%22propertyTypeId%22%2C%22reviewCount%22%2C%22starRating%22%2C%22themeIds%22%2C%22objectID%22%2C%22lastBooked%22%2C%22isDeleted%22%2C%22pricing%22%2C%22sentiments%22%2C%22displayAddress.en%22%2C%22guestSentiments.en%22%2C%22hotelName.en%22%2C%22placeDisplayName.en%22%2C%22placeADName.en%22%2C%22placeDN.en%22%2C%22address.en%22%5D&attributesToHighlight=null&getRankingInfo=true&clickAnalytics=true&aroundLatLng={lat}%2C%20{lng}&aroundRadius=20000&aroundPrecision=2000"
    }

for msg in consumer:
    try:
        topic = msg.topic
        offset = msg.offset
        consumer.commit()
        mongodb = pymongo.MongoClient(MONGO_CONNECTION_STRING)
        db = mongodb[MONGO_DB_NAME]
        hotels = db['FindHotels']

        body = msg.value.decode('utf-8')
        body = json.loads(body)['body']
        result = requests.post(findHotelApiHost, json=generateQuery(body['lat'], body['lng']))
        hits = json.loads(result.content)['hits']
        for hit in hits:
            hotels.update_many({"_id": hit['objectID']}, {"$set": hit}, upsert=True)
    except Exception:
        traceback.print_exc()