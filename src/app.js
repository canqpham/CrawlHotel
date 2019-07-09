var os = require("os");
var request = require('request');
var mongodb = require('mongodb');
var ConsumerGroup = require('kafka-node').ConsumerGroup;

const sizeGet = 1000

const findHotelApiHost = "https://4uygjp42kq-dsn.algolia.net/1/indexes/prod_hotel_v2/query?x-algolia-agent=Algolia%20for%20vanilla%20JavaScript%203.32.0&x-algolia-application-id=4UYGJP42KQ&x-algolia-api-key=efa703d5c0057a24487bc9bdcb597770&x-algolia-usertoken=d00c47a3-6590-4ed2-873b-97e873e205b8"
const generateQuery = (lat, lng) => ({
    params: `page=0&hitsPerPage=${sizeGet}&facets=%5B%22*%22%5D&filters=isDeleted%20%3D%200&facetFilters=%5B%5D&numericFilters=%5B%5D&optionalFilters=%5B%5D&attributesToRetrieve=%5B%22_geoloc%22%2C%22chainId%22%2C%22checkInTime%22%2C%22checkOutTime%22%2C%22facilities%22%2C%22guestRating%22%2C%22guestType%22%2C%22imageURIs%22%2C%22popularity%22%2C%22propertyTypeId%22%2C%22reviewCount%22%2C%22starRating%22%2C%22themeIds%22%2C%22objectID%22%2C%22lastBooked%22%2C%22isDeleted%22%2C%22pricing%22%2C%22sentiments%22%2C%22displayAddress.en%22%2C%22guestSentiments.en%22%2C%22hotelName.en%22%2C%22placeDisplayName.en%22%2C%22placeADName.en%22%2C%22placeDN.en%22%2C%22address.en%22%5D&attributesToHighlight=null&getRankingInfo=true&clickAnalytics=true&aroundLatLng=${lat}%2C%20${lng}&aroundRadius=20000&aroundPrecision=2000`
})

const kafkaTopic = process.env.KAFKA_TOPIC
const topics = [kafkaTopic]
const kafkaHost = process.env.KAFKA_HOST;
const groupId = process.env.KAFKA_CONSUMER_GROUP;
const hostname = os.hostname();

/**
 * Mongo db configuration
 * ======================
 */
const dbName = process.env.DB_NAME || 'Hotels'
const dbConnectionString = process.env.MONGO_CONNECTION_STRING || 'Hotels'

var consumerOptions = {
    kafkaHost,
    groupId,
    sessionTimeout: 15000,
    protocol: ['roundrobin'],
    fromOffset: 'earliest'
};

var consumerGroup = new ConsumerGroup(Object.assign({ id: `${hostname}_${Math.round(Math.random() * 10000)}` }, consumerOptions), topics);
consumerGroup.on('connect', () => console.log(`Listening on kafka host ${kafkaHost} with these topics [${topics.join(',')}]`));
consumerGroup.on('message', onMessage);


/**
 *  Get list hotels from FindHotel by lat and lng
 * 
 */

function onMessage(message) {
    const { body } = JSON.parse(message.value)
    request({
        url: findHotelApiHost,
        method: "POST",
        json: generateQuery(body.lat, body.lng),
    }, (err, res, body) => {
        if (err) {
            console.error(err);
            next();
        }
        else if (body) {

            const { hits } = body;
            if (hits && hits.length > 0) {
                console.log("Found " + hits.length + " items in " + body.city)

                mongodb.connect(dbConnectionString, (err, client) => {
                    var db = client.db(dbName);
                    try {
                        hits.map(async hit => {
                            const data = {
                                ...hit,
                                _id: hit.objectID,
                            }
                            db.collection("FindHotels").updateOne(
                                { _id: data._id },
                                { $set: data },
                                { upsert: true }
                            ).then(result => {
                                console.log(result)
                            }).catch(error => {
                                console.error(error)
                            })
                        })
                    } catch (error) {
                        console.error('Cannot create new hotels');
                    }
                })
            }

        } else {
            console.error('Cannot get data from host');
        }
    });
    console.log('\n\n')
}

