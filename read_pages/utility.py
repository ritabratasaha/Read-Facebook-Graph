#####

import facebook
import boto3.session
import json
import requests
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(message)s')

def set_session():

    ## Generate Boto Session based on .aws/config
    session = boto3.session.Session(profile_name='Dev')
    return session


def get_fb_app_credentials():

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


def get_fb_page_accessToken():

    ## Read existing access token from s3
    bucketName = 'rs.webanalytics'
    inobjectKey = 'Facebook/Tokens/LearnerApp_Token.json'
    session = set_session()
    s3 = session.client('s3')
    obj = s3.get_object(Bucket=bucketName, Key=inobjectKey)
    page_object= json.loads(obj["Body"].read().decode())
    logging.info ("page access object = {0} ".format(page_object))
    return page_object


def refresh_fb_access_token():

    ## Refresh current token
    
    app_id, app_secret = get_fb_app_credentials()
    page_object = get_fb_page_accessToken()
    page_id = page_object['page_id']
    current_token = page_object['access_token']

    url = 'https://graph.facebook.com/oauth/access_token'       
    payload = {
        'grant_type': 'fb_exchange_token',
        'client_id': app_id,
        'client_secret': app_secret,
        'fb_exchange_token': current_token
    }
    response = requests.get(url, params=payload)
        
    if (response.status_code == 200):

        logging.info("graph api response status = {0}".format(response.status_code))
        new_access_token = response.json()["access_token"]
        logging.info ("new access token : {0}".format(new_access_token))
        
        response_dict = response.json()
        page_object.update(response_dict)

        ## Write current token back to S3
        bucketName = 'rs.webanalytics'
        outobjectKey = 'Facebook/Tokens/LearnerApp_Token.json'
        session = set_session()
        s3 = session.client('s3')
        obj = s3.put_object(Bucket=bucketName, Key=outobjectKey, Body= json.dumps(page_object,indent=4))

        if (obj['ResponseMetadata']['HTTPStatusCode'] == 200):
            logging.info("Refresh token uploaded")
        else:
            logging.info("Refresh token upload failed. Exiting processing")
            exit(0)

    else:
        logging.info("Refresh of token failed. Exiting processing")
        exit(0)    

    return page_id, new_access_token 
     

    
    

    
