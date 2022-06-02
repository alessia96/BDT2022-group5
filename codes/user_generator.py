# create random users and save them in the a mongoDB collection called "users"

import pandas as pd
from pymongo import MongoClient as Client
from faker import Faker

# init empty dictionary to save users
users = {'ipv4': [], 'place': []}


for i in range(1000000):
	# create fake persone
	p = Faker()
	# save in the dictionary ipv4 and place of the fake person
	users['ipv4'].append(p.ipv4())
	users['place'].append(p.postcode())

# tranform the dictionary into a pandas dataframe
users = pd.DataFrame(users)

# connect to mongo database
db = Client("mongodb://127.0.0.1/")
db = db.BDT

# save users into "users" collection 
db.users.insert_many(db.to_dict('records'))


