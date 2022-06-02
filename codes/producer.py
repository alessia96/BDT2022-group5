# clickstream kafka producer
#
# replace USERNAME and PASSWORD with your kafka cloud, as CloudKarafka
# if you use a different sasl mechaninsm look at the documentation of your provider


import json
import pandas as pd
import random
from random import randrange, randint
from datetime import timedelta, datetime
from faker import Faker
from confluent_kafka import Producer, KafkaException, KafkaError
import pymongo
from pymongo import MongoClient as Client

# kafka Producer configuration
topics = ['USERNAME-click']
conf = {'bootstrap.servers':  'rocket-01.srvs.cloudkafka.com:9094',
        'security.protocol': 'SASL_SSL',
	'sasl.mechanisms': 'SCRAM-SHA-256',
        'sasl.username': USERNAME,
        'sasl.password': PASSWORD
    }

# create the producer
p = Producer(**conf)

# connect to mongo and load users and articles
client = Client("mongodb://127.0.0.1/")
db = client.BDT
df = pd.DataFrame(db.articles.find()) 
df2 = pd.DataFrame(db.users.find())

# create a random datetime for the click
# used only once for the msg['time'] to have some old data
def random_date(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)


# define start and end time for the random datetime
start_time = datetime(2022, 1, 1, 0, 1, 1)
end_time = datetime(2022, 6, 10, 23, 59)
# specify the datetime format
date_format = '%Y/%m/%d-%H:%M:%S'


# stream data to click topic
while True:
    # create a fake person
    fake = Faker()
    # load a "real person" from the collection
    real = df2.loc[random.randint(0, len(df2)-1)]
    r = [[fake.ipv4(), fake.postcode()], [real.ipv4, real.place]]
    
    # choose randomly the user between existing and fake
    user = r[random.randint(0, 1)]
    
    # kafka message
    msg = {'article':str(df.loc[randint(0, len(df)-1)].link), # article link
           'user-ipv4':str(user[0]), # user ipv4
           'place':str(user[1]), # postalcode of the user
           'time': datetime.strftime(datetime.now(), date_format) # date of the click
           }
    # send message to topic in json format
    p.produce(topics[0], json.dumps(msg).encode('utf-8'))
    p.flush()
    p.poll(0)


