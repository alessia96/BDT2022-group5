# suggestion generator
# run it 1 time per day
# from users clicks generates suggestions based on past behavior, other people behavior and new articles 

import pandas as pd
import pymongo
from pymongo import MongoClient as Client
from datetime import datetime, timedelta


# select client and database
client = Client("mongodb://127.0.0.1/")
db = client.BDT

# save all clicks
clicks = db.click.find()

# to make it easier we can convert the clicks into a pandas dataframe
df_clicks = pd.DataFrame(db.click.find())
df_clicks.place = df_clicks.place.astype(int)


# we want to produce a lists of suggestested articles for each user and place (non existing users)
# if user exists: past behavior, popular (time and place), new
# otherwise: popular (time and place), new
# each list contains 30 links


# time_popular and new are the same for everyone, we can already define them

date_format = '%Y/%m/%d'
# specify the date of one week ago
week_days = datetime.now()-timedelta(days=7)

# from "articles" collection collect all the rows containing a "date" between a week ago and today
# convert it on a list and pick the first 30 entries
last_week_articles = list(db.articles.find( { "date": { "$gt": datetime.strftime(week_days, date_format), "$lte": datetime.strftime(datetime.now(), date_format) } } ))[:30]

# create a list of links of suggested articles
new_sugg = [i['link'] for i in last_week_articles]


# to obtain the most popular in the last week we have to take the last week clicks and then count the frequncy each 
# article has been read. The time_popular suggestion list will contain the 30 links that have been clicked most

# from pandas dataframe of clicks select rows containing a "time" between today and 1 week ago
last_week_clicks = df_clicks.loc[df_clicks.time >= datetime.strftime(datetime.now()-timedelta(days=7), '%Y/%m/%d')]
#initialize empty dictionary to store links and count
# key = article link, value = number of clicks
c = {}
for link in last_week_clicks.article:
	if link not in c:
		c[link] = 1
	else:
		c[link] += 1

# get the 30 most clicked articles
time_pop = [k for k, v in sorted(c.items(), key=lambda item: item[1])][-30:]


# to get place_popular we consider the nearest postcodes 
# consider postalcodes with a step of 100 --e.g. 80100, 80200, ...
# initialize empty dict where we'll save the suggestion for each base postalcode
# key = postalcode, value= list of most clicked articles in that place
place_pop = {} 
for i in range(0, 99900, 100):
	# select the clicks from the given postalcode
	# take postalcode between i and i+100 --e.g. from i=80100 to i+100=80200
	place_click = df_clicks.loc[(df_clicks.place >= i) & (df_clicks.place < i+100)]
	#initialize empty dictionary to store links and count
	# key = article link, value = number of clicks
	c = {}
	for link in place_click.article:
		if link not in c:
			c[link] = 1
		else:
			c[link] += 1
	# save the 30 most clicked articles of the place
	place_pop[str(i)] = [k for k, v in sorted(c.items(), key=lambda item: item[1])][-30:]

	

# function to get the most read category articles of a user
# take as input the user ipv4 and the pandas dataframe of articles
def get_categories(user_ip, articles):
	# initialize empty dictionary where we'll save category and count
	# key = category, value = count
	cat_count = {}
	# select the click rows that have as user-ipv4 the given user_ip
	# select only the article column (contains links)
	for links in df_clicks.loc[df_clicks['user-ipv4'] == user_ip].article:
		# get the category of the article
		# e.g. idx = crime_12345 --> cat = crime
		cat = articles.loc[articles.link == links].idx.values[0][:5]
		if cat in cat_count:
			cat_count[cat] += 1
		else:
			cat_count[cat] = 1
	
	# sort the categories
	sorted_cat = [k for k, v in sorted(cat_count.items(), key=lambda item: item[1])]
	# if there is only 1 category, we add a random category to the list
	if len(sorted_cat) == 1:
		new_c = articles.idx[0][:5] if articles.idx[0][:5] not in sorted_cat else articles.idx[1][:5]
		sorted_cat.append(new_c)
	else:
		sorted_cat = sorted_cat[-2:]
	# initialize empty list where we'll store the articles links
	sugg_art = []
	for c in sorted_cat:
		# for each category add 15 articles	
		sugg_art += list(articles.loc[articles.idx.str.contains(c)].link)[:15]
	# return list of 30 articles
	return sugg_art


# load the articles collection as pandas dataframe
articles = pd.DataFrame(db.articles.find())

# for each user create the suggestions lists and store it in a pandas dataframe
new_suggestions = pd.DataFrame(columns = ['user-ipv4', 'past', 'time_popular', 'place_popular', 'new'])
for user in df_clicks['user-ipv4'].unique()[:5]:
	# get user base-postalcode
	p = round(df_clicks.loc[df_clicks['user-ipv4'] == user].place.values[0] / 100) *100
	
	# create the dictionary with suggestions
	sugg = {'user-ipv4': user,
		'past': get_categories(user, articles),
		'time_popular': time_pop,
		'place_popular': place_pop[str(p)],
		'new': new_sugg}
	# write the suggestion in new_suggestions dataframe
	new_suggestions = new_suggestions.append(sugg, ignore_index=True)
	
# since we can have new users, we have to create suggestions also for them
# they are as those for single users, but in this case we don't have the past behavior suggestion
for postalcode in place_pop:
	# create the dictionary with suggestions
	sugg = {'user-ipv4': str(postalcode),
		'past': None,
		'time_popular': time_pop,
		'place_popular': place_pop[str(postalcode)],
		'new': new_sugg}
	# write the suggestion for the user in new_suggestions dataframe
	new_suggestions = new_suggestions.append(sugg, ignore_index=True)

# since we do not need anymore the old suggestions, we can delete them and write the new ones
db.suggestions.drop()
# save the new suggestions to the collection "suggestions"
db.suggestions.insert_many(new_suggestions.to_dict('records'))

##

# save new users
#load new users
old_users = set(pd.DataFrame(db.users.find())['ipv4'].unique())
# take clicks of last 24 hours
today_users = df_clicks.loc[df_clicks.time >= datetime.strftime(datetime.now()-timedelta(hours=24), '%Y/%m/%d')]

# save new user ipv4 and place in db
for u in today_users:
	if u['user-ipv4'] not in old_users:
		db.users.insert_one({'ipv4': u['user-ipv4'], 'place': u['place']})

