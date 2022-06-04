# BDT2022-group5

**UnitTN Big Data Technologies project 2022 - group 5**

The aim of this big data project is to create a system which sends suggested articles to users, based on their past behavior on a news website.

The main idea is that a news website sends us information about the click it receives, and we send back some article suggestions.

The system uses three main technologies: Kafka, Spark, and MongoDB. All these technologies are used with python. 
Through Kafka the news website sends a message containing information about the clicks: user ipv4, place (indicated with the postal code), and time. 
The system reads the Kafka message through Spark and simultaneously saves this information in a MongoDB collection, and sends, through Kafka a message containing the suggested articles for a given user.
#

**Prerequisites:**
  - Python >= 3.7
  - Spark >= 3.0.3
  - MongoDB >= 4.4
  - Python packages: faker, confluent_kafka, pandas, pymongo, datetime, json, random, pyspark

#
**Articles dataset source**: https://www.kaggle.com/datasets/rmisra/news-category-dataset?select=News_Category_Dataset_v2.json

#
**Running the code**

Before starting  the system, run the file user_generator.py. It produces and saves n fake users in a MongoDB collection called “users”. We will use it when generating the clicks in order to have some existing users. Fake users are generated through Faker python  package.
To activate the system, run, in different prompt/terminal windows, the following python script:
  - producer.py: produces fake clicks simulating the activity in a website. Every time a user visit a page, a message is sent through Kafka to the topic       “clicks”
  - click_streaming.py: reads the Kafka stream through Spark (pyspark) and simultaneously save the click information in a MongoDB collection called             “click”, and send to the Kafka topic “give_suggestions” a message containing a list of 5 suggested articles for a given user identified by its ipv4.
  - suggestion_generator.py: generates suggestions for each existing user and also for non-existing users (consider the postal code). It is run each 24         hour. 
  - consumer.py: receive and read the suggestions sent with click_streaming.py. It is a way to see what we send to the news company

