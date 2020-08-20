#####
# Read data from Facebook Graph API
####

import json
import facebook
import logging
import requests
from  utility import refresh_fb_access_token,set_session,get_fb_page_current_accessToken
from datetime import *



def get_page_insights(page_id,access_token,from_date,to_date):

    
    metric_list = ['page_views_total','page_views_by_age_gender_logged_in_unique','page_posts_impressions',
                   'page_posts_impressions_organic','page_posts_impressions_paid','page_engaged_users',
                   'page_post_engagements','page_engaged_users','page_post_engagements','post_reactions_by_type_total',
                   'post_reactions_anger_total','post_reactions_sorry_total','post_reactions_haha_total',
                   'post_reactions_wow_total','post_reactions_love_total','post_reactions_like_total',
                   'page_fans','page_fans_country','page_fans_city','page_fans_gender_age','page_fans_by_like_source',
                   'page_fan_removes','post_impressions_paid_unique','post_impressions_organic_unique',
                    'post_impressions_viral_unique','page_total_actions','page_consumptions']
    param_metric = ''
    for items in metric_list:
        param_metric =  items + ',' + param_metric
    param_metric = param_metric.rstrip(',')

    url = 'https://graph.facebook.com/v8.0/' + page_id + '/insights'       
    payload = {
        'access_token': access_token,
        'since' : from_date,
        'until' : to_date,
        'period' : 'day',
        'metric': param_metric
    }
    logging.info (" GET request url : '{0}' and parameters : '{1}'".format(url,payload))
    response = requests.get(url, params=payload)
    #print(json.dumps(response.json(), indent=4))
    return response.status_code, response.json()



def main():

    
    ## Generate new token
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

        bucketName = 'rs.webanalytics'
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
   
    
    ## Pull data from API

    startdate = datetime.strptime('2020-08-10','%Y-%m-%d').date()

    for i in range(2): # No days from the starting date
        
        unixdate = int(startdate.strftime("%s"))
        filedate = startdate.strftime("%Y%m%d")
        logging.info("Current processing date : {0}->{1}".format(startdate, unixdate))
        
        ## Call page impressions
        status_code, response_json = get_page_insights(page_id,new_access_token, unixdate, unixdate)

        if (status_code == 200):
            
            filename = 'Insights_' + page_id + '_' + filedate + '.json'
            ## Write raw response to S3
            bucketName = 'rs.webanalytics'
            outobjectKey = 'Facebook/Raw_Store/'+ filename
            session = set_session()
            s3 = session.client('s3')
            obj = s3.put_object(Bucket=bucketName, Key=outobjectKey, Body= json.dumps(response_json,indent=4))
            if (obj['ResponseMetadata']['HTTPStatusCode'] == 200):
                logging.info("Impression file {0} uploaded".format(filename))
            else:
                logging.info("Impression file uploading failed. Exiting processing")
                exit(0)
        else:
            logging.info ("API response error : {0}".format(response_json))
        
        # increment to the next day
        next_day = startdate + timedelta(days=1)
        startdate = next_day


if __name__ == "__main__":
    main()
