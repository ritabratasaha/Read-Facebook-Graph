##### Set of Functions

import facebook
import boto3.session
import json
import requests
import logging
import csv

logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(message)s')

def set_session():

    ## Generate Boto Session based on .aws/config
    session = boto3.session.Session(profile_name='Dev')
    return session


def get_fb_app_credentials():

    logging.info("Function get_fb_app_credentials() invoked")
    ## Read existing access token from s3
    bucketName = 'rs.webanalytics'
    inobjectKey = 'Facebook/App_Credentials.json'
    session = set_session()
    s3 = session.client('s3')
    obj = s3.get_object(Bucket=bucketName, Key=inobjectKey)
    jsondata = json.loads(obj["Body"].read().decode())
    app_id = jsondata["app_id"]
    app_secret = jsondata["app_secret"]
    return app_id, app_secret


def get_fb_page_current_accessToken():

    logging.info("Function get_fb_page_current_accessToken() invoked")
    ## Read existing access token from s3
    bucketName = 'rs.webanalytics'
    inobjectKey = 'Facebook/Tokens/LearnerApp_Token.json'
    session = set_session()
    s3 = session.client('s3')
    obj = s3.get_object(Bucket=bucketName, Key=inobjectKey)
    page_access_object= json.loads(obj["Body"].read().decode())
    logging.info ("Current page access object = {0} ".format(page_access_object))
    return page_access_object


def refresh_fb_access_token():

    logging.info("Function refresh_fb_access_token() invoked")
    ## Refresh current token
    
    app_id, app_secret = get_fb_app_credentials()
    page_access_object = get_fb_page_current_accessToken()
    current_token = page_access_object['access_token']

    url = 'https://graph.facebook.com/oauth/access_token'       
    payload = {
        'grant_type': 'fb_exchange_token',
        'client_id': app_id,
        'client_secret': app_secret,
        'fb_exchange_token': current_token
    }
    response = requests.get(url, params=payload)
        
    return response.status_code, response.json()
     


def page_insight_create_csv(bucket_name, object_key):
    
    logging.info("Function page_insight_create_csv() invoked")
    file_name = object_key.split('/')[-1].split('.')[0]
    inobjectKey = object_key
    outobject_key =  'Facebook/Csv_Store/' + file_name + '.csv'
    csv_file_name = '/tmp/' + file_name + '.csv'
  
    session = set_session()
    session = boto3.session.Session(profile_name='Dev')
    s3 = session.client('s3')
    s3_obj = s3.get_object( Bucket= bucket_name , Key= inobjectKey)
    s3_objdata = s3_obj['Body'].read().decode('utf-8')
    access_dict = json.loads(s3_objdata)

    with open (csv_file_name,"w") as file:
        csv_file = csv.writer(file,quotechar='"',quoting=csv.QUOTE_ALL)
        csv_file.writerow(["Metric","Values","EffectiveDate"])
        for metric in access_dict['data']:
            Metric = metric['name']
            for values in metric['values']:
                mvalues = values['value']
                endtime = (values['end_time'])[:10]
                csv_file.writerow([Metric,mvalues,endtime])

    logging.info("Upload params : csv file name {0} uploaded object {1}".format(csv_file_name, outobject_key))

    try :
        s3.upload_file(csv_file_name,bucket_name,outobject_key)
        logging.info("File has been uploaded")
    except ClientError as e:
        logging.error(e)
        return False

    return True

def post_insight_create_csv(bucket_name, object_key):

    logging.info("Function post_insight_create_csv() invoked")
    file_name = object_key.split('/')[-1].split('.')[0]
    inobjectKey = object_key
    outobject_key =  'Facebook/Csv_Store/' + file_name + '.csv'
    csv_file_name = '/tmp/' + file_name + '.csv'
  
    session = set_session()
    session = boto3.session.Session(profile_name='Dev')
    s3 = session.client('s3')
    s3_obj = s3.get_object( Bucket= bucket_name , Key= inobjectKey)
    s3_objdata = s3_obj['Body'].read().decode('utf-8')
    access_dict = json.loads(s3_objdata)

    with open (csv_file_name,"w") as file:
        csv_file = csv.writer(file,quotechar='"',quoting=csv.QUOTE_ALL)
        csv_file.writerow(["Id","Metric","Period","Values","Title"])
        for items in access_dict:
            Metric = items['name']
            Period = items['period']
            Values = items['values']
            Title = items['title']
            Id = items['id']
            csv_file.writerow([Id,Metric,Period,Values,Title])

    logging.info("Upload params : csv file name {0} uploaded object {1}".format(csv_file_name, outobject_key))

    try :
        s3.upload_file(csv_file_name,bucket_name,outobject_key)
        logging.info("File has been uploaded")
    except ClientError as e:
        logging.error(e)
        return False

    return True