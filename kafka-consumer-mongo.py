# Import some necessary modules
# pip install kafka-python
# pip install pymongo
# pip install "pymongo[srv]"
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.server_api import ServerApi

import json

uri = 'mongodb+srv://leiferTorres:h7D7B1EhlJQxgub7@cluster0.07nwh.mongodb.net/dbapi?retryWrites=true&w=majority'
#uri= 'mongodb://127.0.0.1:27017/memes'
# Create a new client and connect to the server
#client = MongoClient(uri, server_api=ServerApi('1'))
# Send a ping to confirm a successful connection

#try:
#    client.admin.command('ping')
#    print("Pinged your deployment. You successfully connected to MongoDB!")
#except Exception as e:
#    print(e)

# Connect to MongoDB and pizza_data database

try:
    client = MongoClient(uri)
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")

    db = client.memes
    print("MongoDB Connected successfully!")
except:
    print("Could not connect to MongoDB")

consumer = KafkaConsumer('test',bootstrap_servers=[
    'my-kafka-0.my-kafka-headless.emilianotorres123.svc.cluster.local:9092'
    #'localhost:9092'
    ])

#count
try:
    agg_result=db.memes_info.aggregate(
    [
        {
        "$group":
        {
            "_id":"$name",
            "n":{"$sum":1}
        }}
    ])
    db.memes_summary.delete_many({})
    for i in agg_result:
        print(i)
        summary_id= db.memes_summary.insert_one(i)
        print("Summary inserted  with records  ids",summary_id)
except Exception as e:
    print(f'Group by caught {type(e)}:')
    print(e)

# Parse received data from Kafka
for msg in consumer:
    record = json.loads(msg.value)
    print(record)
    name = record['name']

    # Create dictionary and ingest data into MongoDB
    try:
       meme_rec = {'name':name }
       print (meme_rec)
       meme_id = db.memes_info.insert_one(meme_rec)
       print("Data inserted with record ids", meme_id)
    except:
       print("Could not insert into MongoDB")
