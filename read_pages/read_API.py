#####
# Read data from Facebook Graph API
####

import json
import facebook
import logging
import requests
from  utility import refresh_fb_access_token,set_session,get_fb_page_current_accessToken,page_insight_create_csv,post_insight_create_csv
from page_insights import get_page_insights
from post_insights import get_post_insights
from page_metrics import get_page_metrics
from datetime import *


def main():

    
    bucketName = 'rs.webanalytics'

    ## 1. Generate "new token"
    status_code, response_json  = refresh_fb_access_token()
    if (status_code == 200):
        logging.info("Refresh token response status = {0}".format(status_code))
        #logging.info ("new access token : {0}".format(new_access_token))
        page_access_object = get_fb_page_current_accessToken()
        page_id = page_access_object['page_id']
        new_access_token = response_json['access_token']
        ## Overwrite current page access token with new token
        page_access_object.update(response_json)
        logging.info ("Updated page access object with refreshed token = {0} ".format(page_access_object))
        outobjectKey = 'Facebook/Tokens/LearnerApp_Token.json'
        session = set_session()
        s3 = session.client('s3')
        obj = s3.put_object(Bucket=bucketName, Key=outobjectKey, Body= json.dumps(page_access_object,indent=4))
        if (obj['ResponseMetadata']['HTTPStatusCode'] == 200):
            logging.info("Refresh token uploaded")
        else:
            logging.info("Refresh token upload failed. Exiting processing")
            exit(0)
    else:
        logging.info("Refresh of token failed. Exiting processing")
        exit(0)    
   


   
    ## 1. Pull "page metrics" from API
    ## 2. Store reponse in raw store as json
    ## 3. Convert json to csv
    status_code, response_json = get_page_metrics(page_id,new_access_token)
    if (status_code == 200):
        logging.info("Response from page metrics{0}".format(response_json))
        filedate = date.today().strftime("%Y%m%d")
        filename = 'PageMetric_' + page_id + '_' + filedate + '.json'
        outobjectKey = 'Facebook/Raw_Store/'+ filename
        session = set_session()
        s3 = session.client('s3')
        obj = s3.put_object(Bucket=bucketName, Key=outobjectKey, Body= json.dumps(response_json,indent=4))
        if (obj['ResponseMetadata']['HTTPStatusCode'] == 200):
            logging.info("Page file {0} uploaded".format(filename))
        else:
            logging.info("Page file uploading failed. Exiting processing")
            exit(0)
        # Convert to CSV (TBD)
    else:
        logging.info("Response from page metrics failed")
    




    ## 1. Pull "page insights" data from API
    ## 2. Store reponse in raw store as json
    ## 3. Convert json to csv
    startdate = datetime.strptime('2020-08-15','%Y-%m-%d').date()
    for cnt in range(2): # No days from the starting date. Loop through each day
        unixdate = int(startdate.strftime("%s"))
        filedate = startdate.strftime("%Y%m%d")
        logging.info("Current processing date : {0}->{1}".format(startdate, unixdate))
        ## Call page impressions
        status_code, response_json = get_page_insights(page_id,new_access_token, unixdate, unixdate)
        if (status_code == 200):
            filename = 'PageInsights_' + page_id + '_' + filedate + '.json'
            ## Write raw response to S3
            outobjectKey = 'Facebook/Raw_Store/'+ filename
            session = set_session()
            s3 = session.client('s3')
            obj = s3.put_object(Bucket=bucketName, Key=outobjectKey, Body= json.dumps(response_json,indent=4))
            if (obj['ResponseMetadata']['HTTPStatusCode'] == 200):
                logging.info("Page Insights file {0} uploaded".format(filename))
            else:
                logging.info("Page Insights file uploading failed. Exiting processing")
                exit(0)
        else:
            logging.info ("API response error : {0}".format(response_json))
        
        # increment to the next day
        next_day = startdate + timedelta(days=1)
        startdate = next_day
        ## Change JSON to CSV and move to CSV folder
        status = page_insight_create_csv(bucketName, outobjectKey)
        if ( status == True ):
            logging.info("Conversion to csv successful ")
        else :
            logging.info("Conversion to csv failed ")
            exit(0)



    ## Pull "post insights" data from API
    startdate = date.today() - timedelta(days=30)
    unixdate = int(startdate.strftime("%s"))
    status_code, post_insights_json =  get_post_insights(page_id,new_access_token,unixdate)
    #print(json.dumps(post_insights,indent=4))
    if (status_code == 200):
            filedate = date.today().strftime("%Y%m%d")
            filename = 'PostInsights_' + page_id + '_' + filedate + '.json'
            ## Write raw response to S3
            outobjectKey = 'Facebook/Raw_Store/'+ filename
            session = set_session()
            s3 = session.client('s3')
            obj = s3.put_object(Bucket=bucketName, Key=outobjectKey, Body= json.dumps(post_insights_json,indent=4))
            if (obj['ResponseMetadata']['HTTPStatusCode'] == 200):
                logging.info("Post Insights file {0} uploaded".format(filename))
            else:
                logging.info("Post Insights file uploading failed. Exiting processing")
                exit(0)
    else:
        logging.info ("Error in Posts insight call ")

    ## Change JSON to CSV and move to CSV folder
    status = post_insight_create_csv(bucketName, outobjectKey)
    if ( status == True ):
        logging.info("Conversion to csv successful ")
    else :
        logging.info("Conversion to csv failed ")
        exit(0)


if __name__ == "__main__":
    main()
