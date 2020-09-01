
import boto3
import json
import requests
from datetime import *
import time
import pandas as pd
import csv
import logging
#from datetime import timedelta 


  
bucketName = 'rs.spark'
inobjectKey = 'Facebook/Tokens/LearnerApp_Token.json'
session = boto3.session.Session(profile_name='Dev')
s3 = session.client('s3')

all_objects = s3.list_objects(Bucket = bucketName, Prefix = '2020/', Delimiter='/') 

#print(all_objects['Contents'])

for keys in all_objects['Contents']:
    print(keys['Key'])



#s3.upload_file('/tmp/AccountInsights_17841440809241978_20200827.csv',bucketName,'2021/AccountInsights_17841440809241978_20200827.csv')

# folder  =  datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
# client = boto3.client('s3')

# response = client.put_object(
#         Bucket='ritabrata.imdb.2018',
#         Body='',
#         Key= folder+'/'
#         )

text = '20200801'
print(text[4:6])



