import json
import requests
import logging
import boto3
from  utility import refresh_fb_access_token,set_session,get_fb_page_current_accessToken
import csv

def get_page_insights(page_id,access_token,from_date,to_date):

    # Ref : https://developers.facebook.com/docs/graph-api/reference/v8.0/insights

    metric_list = ['page_views_total','page_views_by_age_gender_logged_in_unique','page_posts_impressions',
                   'page_posts_impressions_organic','page_posts_impressions_paid','page_engaged_users',
                   'page_post_engagements','page_engaged_users','page_post_engagements','post_reactions_by_type_total',
                   'post_reactions_anger_total','post_reactions_sorry_total','post_reactions_haha_total',
                   'post_reactions_wow_total','post_reactions_love_total','post_reactions_like_total',
                   'page_fans','page_fans_country','page_fans_city','page_fans_gender_age','page_fans_by_like_source',
                   'page_fan_removes','page_total_actions','page_consumptions']
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




def page_insight_create_csv(bucket_name, object_key):

    #Inputs: bucketname of the source file that needs to be converted.
    #        object key of the source file that needs to be converted. 
    
    logging.info("Function page_insight_create_csv() invoked")
    file_name = object_key.split('/')[-1].split('.')[0]
    #year = file_name.split('_')[-1][:4]
    #month = file_name.split('_')[-1][4:6]
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
        csv_file.writerow(["Id","Metric","Values","EffectiveDate"])
        for metric in access_dict['data']:
            Id = metric['id']
            Metric = metric['name']
            for values in metric['values']:
                mvalues = values['value']
                endtime = (values['end_time'])[:10]
                csv_file.writerow([Id,Metric,mvalues,endtime])

    logging.info("Upload params : csv file name {0} uploaded object {1}".format(csv_file_name, outobject_key))

    try :
        s3.upload_file(csv_file_name,bucket_name,outobject_key)
        logging.info("File has been uploaded")
    except ClientError as e:
        logging.error(e)
        return False

    return True