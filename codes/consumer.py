# read and print the suggestions
# each message contains the ipv4 of the user and a list of suggested articles (links)

# replace USERNAME and PASSWORD with your kafka cloud, as CloudKarafka
# if you use a different sasl mechaninsm look at the documentation of your provider

import json
import pandas as pd
import random
from random import randrange, randint
from datetime import timedelta, datetime

# Consumer configuration
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError

topics = ['USERNAME-give_suggestion']
conf = {'bootstrap.servers': 'rocket-01.srvs.cloudkafka.com:9094',
        'group.id': "%s-consumer" % USERNAME,
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'security.protocol': 'SASL_SSL',
	'sasl.mechanisms': 'SCRAM-SHA-256',
        'sasl.username': USERNAME,
        'sasl.password': PASSWORD
    }

# define the consumer
c = Consumer(**conf)
# subscribe to insterested topics
c.subscribe(topics)

# read and print the messages
while True:
    msg = c.poll(timeout=0.5)
    if msg is None:
        continue
    else:
        print(msg.value())

