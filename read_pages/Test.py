
import boto3
import json
import requests
from datetime import *
import time
#from datetime import timedelta 
   
""" bucketName = 'rs.webanalytics'
inobjectKey = 'Facebook/Tokens/LearnerApp_Token.json'
session = boto3.session.Session(profile_name='Dev')
s3 = session.client('s3')
obj = s3.get_object(Bucket=bucketName, Key=inobjectKey)
page_object= json.loads(obj["Body"].read().decode())
        
print (page_object)
upd_dict = {"access_token": "test"}
page_object.update(upd_dict)
print (page_object)


url = 'https://graph.facebook.com/oauth/access_token'       
payload = {
        'grant_type': 'fb_exchange_token',
        'client_id': '611417473073068',
        'client_secret': '39b76b9499d3623d4a31b6ed9a200c7d',
        'fb_exchange_token': 'EAAIsFLlKv6wBALZCG9uabtfFg1BrA1elJizSxKRZCwTlQ5EMwq1r1f1RNijUxEzGi2ISz644ZB7sWBjjDrY8uWZAnu55IaZCN33245cYbZAWVZCb9ZCuVzVqgHMXa3ait8vWTugr7JBxUYdbuGd4wk2UXNxxXHi0NV8b1rPd1iV5ZAAZDZD'
    }
response = requests.get(url, params=payload)
#response_dict = json.loads(response.text)
response_dict = response.json()
page_object.update(response_dict)
print(json.dumps(page_object, indent=4)) """



""" utctoday = datetime.utcnow().date()
print(utctoday)
today = date.today()
print(today)
yesterday = (today - timedelta(days=1) )
print(yesterday)
print(int(yesterday.strftime("%s")))
print(int(today.strftime("%s"))) """



metric_list = ['page_views_total','page_views_by_age_gender_logged_in_unique','page_posts_impressions','page_posts_impressions_organic','page_posts_impressions_paid']

concat_list = ''
for items in metric_list:
    concat_list =  items + ',' + concat_list
print (concat_list.rstrip(','))


today = date.today().strftime("%Y%m%d")
print(today)