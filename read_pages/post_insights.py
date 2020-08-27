import json
import requests
import logging
import boto3
from  utility import refresh_fb_access_token,set_session,get_fb_page_current_accessToken
import csv

def get_post_insights(page_id,access_token,from_date):

    # Pull all posts for the page
    logging.info("Function get_post_insights() invoked")
    url = 'https://graph.facebook.com/v8.0/' + page_id  + '/posts'    
    payload = {
        'access_token': access_token,
        'since' : from_date,
        'limit' : 5
        }
    logging.info (" GET request url : '{0}' and parameters : '{1}'".format(url,payload))
    response = requests.get(url, params=payload)
    posts = []

    # Loading all posts into posts[] dictionary
    if ((response.status_code == 200) and (len(response.json()['data']) != 0)):
        posts = response.json()['data']
        paging = response.json()['paging']
        while ('next' in paging):
            next = paging['next']
            response = requests.get(next)
            posts_subset = response.json()['data']
            paging = response.json()['paging']
            posts.extend(posts_subset)
        logging.info("Total Posts dict length : {0}".format(len(posts)))
    else:
        logging.info('Null Respose, there are no posts in this period')


    # Iterate over a valid post[] dict
    if (len(posts) != 0):
        post_insights = [] 
        # Pull post insights for each post id in post[]
        for count in range(len(posts)):
            post_id = posts[count]['id']
            metric_list = ['post_impressions','post_impressions_unique', 'post_impressions_paid_unique' , 
                            'post_impressions_organic_unique', 'post_impressions_viral_unique', 'post_reactions_like_total',
                            'post_reactions_love_total','post_reactions_wow_total','post_reactions_haha_total',
                            'post_reactions_sorry_total','post_reactions_anger_total','post_reactions_by_type_total']
            param_metric = ''
            for items in metric_list:
                param_metric =  items + ',' + param_metric
            param_metric = param_metric.rstrip(',')
            url = 'https://graph.facebook.com/v8.0/' + post_id  + '/insights' 
            payload = {
                'access_token': access_token,
                'limit' : 100,
                'metric': param_metric
            }
            #logging.info (" GET request url : '{0}' and parameters : '{1}'".format(url,payload))
            post_insight_response = requests.get(url, params=payload)

            if ((post_insight_response.status_code == 200) and (len(post_insight_response.json()['data']) != 0)):
                post_insights.extend(post_insight_response.json()['data'])
            else:
                logging.info('Null Respose, there are no insights for this post id ')

            count = count + 1
    
    return post_insight_response.status_code ,post_insights



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