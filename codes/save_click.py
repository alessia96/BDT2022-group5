# if get error trying to run, copy and paste the line below (remember to change the path!)
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /PATH/save_click.py

# read Kafka messages through Spark (pyspark). 
# Spark send back suggestions to Kafka. Suggestions are loaded from a mongodb collection
# Spark write Kafka message infos (clicks) into mongodb collection


import pymongo
from pymongo import MongoClient as Client
import json
import pandas as pd
import random
from random import randrange, randint
from datetime import timedelta, datetime
from faker import Faker
from confluent_kafka import Producer, KafkaException, KafkaError
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql import SQLContext


# define the spark session and add the required packages
spark = SparkSession.\
        builder.\
        config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3").\
        config("spark.mongodb.input.uri", "mongodb://localhost:127.0.0.1/BDT.clicks").\
        config("spark.mongodb.input.readPreference.name", "secondaryPreferred").\
        config("spark.mongodb.output.uri", "mongodb://127.0.0.1/BDT.clicks").\
        appName("streamingExampleWrite").\
        getOrCreate()

# set the level of "alert" to warning
spark.sparkContext.setLogLevel("WARN")

# define the schema
# save as json 
schema = StructType([
    StructField("article", StringType(), True), # article field as string
    StructField("user-ipv4", StringType(), True), # user-ipv4 field as string
    StructField("place", StringType(), True), # place (postalcode) field as string
    StructField("time", StringType(), True) # time field as string
    ])


kafkaServer="rocket-01.srvs.cloudkafka.com:9094"

# define the spark dataframe
# read all the kafka streams of the topic "clicks"
df = (spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafkaServer)
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username='{}' password='{}';".format(USERNAME, PASSWORD))
  .option("kafka.ssl.endpoint.identification.algorithm", "https")
  .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
  .option("subscribe", "ld4r2gg1-click")
  .option("startingOffsets", "latest")
  .option("failOnDataLoss", "false")
  .load().select(from_json(col("value").cast("string"), schema).alias("value")).select("value.*"))


# define a function that write the kafka streams to mongodb
def write_to_mongo(df, b):
	df.write \
	.format("mongo") \
	.option("spark.mongodb.output.uri", "mongodb://127.0.0.1/BDT.click") \
	.mode("append") \
	.save()


# define function to obtain nearest place (postalcode)
# we'll use it later to find suggestions
def near_place(postalcode):
	# take the postal code and format it in a way that all postal codes have 5 digits and go 100 by 100
	# e.g. 80125 -> 80100
	return str(round(int(postalcode)/100)*100)

# connect to mongo and select BDT database
client = Client("mongodb://127.0.0.1/")
db = client.BDT

# kafka Producer configuration
conf = {'bootstrap.servers': 'rocket-01.srvs.cloudkafka.com:9094',
	'security.protocol': 'SASL_SSL',
	'sasl.mechanisms': 'SCRAM-SHA-256',
	'sasl.username': USERNAME,
	'sasl.password': PASSWORD}
producer = Producer(**conf)



# define function to query suggestions collection and send message on kafka topic "give_suggestion"
def get_suggestions(df, b):
	batch = df.collect() # read the batch
	# for each message in the batch
	for i in range(len(batch)):		
		# find user in "suggestions" collection in "BDT" database
		
		# current page (link)
		current_article = batch[i]['article']
		
		# if user does not exists in suggestions colelction
		u = db.suggestions.find({"user-ipv4": batch[i]['user-ipv4']})
		row = list(u)
		if len(row) == 0:
			# user is defined by the place
			# take nearest place -- e.g. 80230 -> 80200
			user = list(db.suggestions.find({"user-ipv4":near_place(batch[i]['place'])}))[0]
			# remove current article if in suggestion		
			if current_article in user['time_popular']:
				user['time_popular'].remove(current_article)
			if current_article in user['place_popular']:
				user['place_popular'].remove(current_article)
			if current_article in user['new']:
				user['new'].remove(current_article)
		else:
			user = row[0]
			if current_article in user['time_popular']:
				user['time_popular'].remove(current_article)
				# remove also from collection row
				db.suggestion.update_one({"_id": user["_id"]}, {"$set":{'time_popular': user['time_popular']}}) 
			if current_article in user['place_popular']:
				user['place_popular'].remove(current_article)
				db.suggestion.update_one({"_id": user["_id"]}, {"$set":{'place_popular': user['place_popular']}}) 
			if current_article in user['new']:
				user['new'].remove(current_article)
				db.suggestion.update_one({"_id": user["_id"]}, {"$set":{'new': user['new']}})  
			if current_article in user['past']:
				user['past'].remove(current_article)
				db.suggestion.update_one({"_id": user["_id"]}, {"$set":{'past': user['past']}}) 
		
		# define the suggestion list
		# base list contains 1 new article, 1 time_popular article, and 1 place_popular article
		# it could happen that an existing user starts to click on all the suggestion and that the lists ends
		# if a list is empty, it is simply ignored and we add a link from another list
		# if all the lists are empty, there are no more suggestions (120 clicks)
		suggestions = []
		n = 0
		if len(user['new']) == 0:
			n += 1
		else:
			suggestions.append(user['new'].pop())
		# consider the suggestion to be added at base
		n += 1
		if len(user['time_popular']) == 0:
			n += 1
		else:
			if n <= len(user['time_popular']):
				while n > 0:
					suggestions.append(user['time_popular'].pop())
					n -= 1
			else:
				while len(user['time_popular']) > 0:
					suggestions.append(user['time_popular'].pop())
		n += 1
		if len(user['place_popular']) == 0:
			n += 1
		else:
			if n <= len(user['place_popular']):
				while n > 0:
					suggestions.append(user['place_popular'].pop())
					n -= 1
			else:
				while len(user['place_popular']) > 0:
					suggestions.append(user['place_popular'].pop())
		
		# if user exists add to the list of suggestions 2 articles based on its past behavior
		if len(row) > 0:
			n += 2
			if n <= len(user['past']):
				while n > 0:
					suggestions.append(user['past'].pop())
					n -= 1
			else:
				while len(user['past']) > 0:
					suggestions.append(user['past'].pop())
		# else add to the list of suggestions 1 time_popular article and 1 place_popular article
		else:
			# if the user does not exists the suggestions are not removed from the collection
			suggestions.append(user['time_popular'].pop())
			suggestions.append(user['place_popular'].pop())
		
		# send to kafka topic "give_suggestion" the suggested articles
		# define message
		msg = {'user-ipv4': batch[i]['user-ipv4'], # user ipv4
			   'suggestions': suggestions} # list of artcile links  
		# send message to "give_suggestion" topic
		producer.produce('USERNAME-give_suggestion', json.dumps(msg).encode('utf-8'))
		producer.flush()
		producer.poll(0)




# write the kafka streams to mongodb and send suggestions
query = df\
    .writeStream \
    .foreachBatch(write_to_mongo)\
    .foreachBatch(get_suggestions)\
    .start()


# we can set a timeout in order to stop the streaming after some time -- query.awaitTermination(timeout=30)
query.awaitTermination() 
query.stop()


