#####
# Read data from Facebook Graph API
####

import json
import facebook
import logging
import requests
from  utility import refresh_fb_access_token,set_session
from datetime import *



def get_page_insights(page_id,access_token):

    today = date.today()
    unixtime_today = int(today.strftime("%s"))
    metric_list = ['page_views_total','page_views_by_age_gender_logged_in_unique','page_posts_impressions',
                   'page_posts_impressions_organic','page_posts_impressions_paid','page_engaged_users',
                   'page_post_engagements']
    param_metric = ''
    for items in metric_list:
        param_metric =  items + ',' + param_metric
    param_metric = param_metric.rstrip(',')

    url = 'https://graph.facebook.com/v8.0/' + page_id + '/insights'       
    payload = {
        'access_token': access_token,
        'since' : unixtime_today,
        'period' : 'day',
        'metric': param_metric
        
    }
    logging.info (" GET request url : '{0}' and parameters : '{1}'".format(url,payload))
    response = requests.get(url, params=payload)
    #print(json.dumps(response.json(), indent=4))
    return response.status_code, response.json()



def main():

    filedate = date.today().strftime("%Y%m%d")
    ## Generate new token
    page_id,access_token = refresh_fb_access_token()
    
    ## Call page impressions
    status_code, response_json = get_page_insights(page_id,access_token)
    if (status_code == 200):
        filename = 'Insights_' + page_id + '_' + filedate + '.json'
         ## Write raw response to S3
        bucketName = 'rs.webanalytics'
        outobjectKey = 'Facebook/Raw_Store/'+ filename
        session = set_session()
        s3 = session.client('s3')
        obj = s3.put_object(Bucket=bucketName, Key=outobjectKey, Body= json.dumps(response_json,indent=4))
        if (obj['ResponseMetadata']['HTTPStatusCode'] == 200):
            logging.info("Impression file uploaded")
        else:
            logging.info("Impression file uploading failed. Exiting processing")
            exit(0)
    else:
        logging.info ("API response error : {0}".format(response_json))
        


if __name__ == "__main__":
    main()
