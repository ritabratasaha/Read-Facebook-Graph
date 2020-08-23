
import boto3
import json
import requests
from datetime import *
import time
import pandas as pd
import csv
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

""" concat_list = ''
for items in metric_list:
    concat_list =  items + ',' + concat_list
print (concat_list.rstrip(','))
 """

# today = date.today().strftime("%Y%m%d")
# print(today)


""" startdate = datetime.strptime('2020-08-01','%Y-%m-%d').date()
print(startdate)

for i in range(10):
    next_day = startdate + timedelta(days=1)
    unixdate = int(next_day.strftime("%s"))
    startdate = next_day
    print(next_day, unixdate)

unixtime = int(startdate.strftime("%s"))
print(unixtime) """


# f = open('/home/ubuntu/data_files/Insights_671278940149620_20200810.json',"r")
# access_dict = json.loads(f.read())


# with open ('/home/ubuntu/data_files/output',"w") as file:
#     csv_file = csv.writer(file,quotechar='"',quoting=csv.QUOTE_ALL)
#     csv_file.writerow(["Metric","Values","EffectiveDate"])
#     for metric in access_dict['data']:
#         Metric = metric['name']
#         for values in metric['values']:
#             mvalues = values['value']
#             endtime = (values['end_time'])[:10]
#             csv_file.writerow([Metric,mvalues,endtime])



session = boto3.session.Session(profile_name='Dev')
bucketName = 'rs.webanalytics'
objectKey = 'Facebook/Raw_Store/'
json_file_name = 'Insights_671278940149620_20200810.json'
file_name = json_file_name.split('.')[0]

session = boto3.session.Session(profile_name='Dev')
s3 = session.client('s3')
s3_obj = s3.get_object( Bucket=bucketName , Key= (objectKey + json_file_name))
s3_objdata = s3_obj['Body'].read().decode('utf-8')
access_dict = json.loads(s3_objdata)

print(file_name)
print(type(access_dict))
#print(json.dumps(access_dict,indent=4))

with open ('/tmp/Insights_671278940149620_20200810.csv',"w") as file:
    csv_file = csv.writer(file,quotechar='"',quoting=csv.QUOTE_ALL)
    csv_file.writerow(["Metric","Values","EffectiveDate"])
    for metric in access_dict['data']:
        Metric = metric['name']
        for values in metric['values']:
            mvalues = values['value']
            endtime = (values['end_time'])[:10]
            csv_file.writerow([Metric,mvalues,endtime])

#s3.put_object(Bucket = bucketName, Key = ('Facebook/Csv_Store/'+ file_name + '.csv'), Body = ('/tmp/'+ file_name + '.csv'))
#s3.put_object(Bucket = bucketName, Key = 'Facebook/Csv_Store/Insights_671278940149620_20200810.csv', Body = '/tmp/Insights_671278940149620_20200810.csv')
response  = s3.upload_file('/tmp/Insights_671278940149620_20200810.csv', 'rs.webanalytics' ,'Facebook/Csv_Store/Insights_671278940149620_20200810.csv')

print (type(response))

