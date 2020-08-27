

import json
import requests
import logging
import boto3
from  utility import refresh_fb_access_token,set_session,get_fb_page_current_accessToken
import csv
from datetime import *



def get_instgm_account_insights(fb_page_id ,access_token, from_date, to_date):
    
    # Get business account id
    # For the business account pull insights.

    logging.info("Function get_instgm_account_insights() invoked")
    url = 'https://graph.facebook.com/v8.0/' + fb_page_id 
    payload = {
        'access_token': access_token,
        'limit' : 100,
        'fields' : 'instagram_business_account'
        }
    logging.info (" GET request url : '{0}' and parameters : '{1}'".format(url,payload))
    response = requests.get(url, params=payload)
    
    instagram_business_account = response.json()['instagram_business_account']['id']

    metric_list = ['impressions','reach','follower_count','profile_views']
    param_metric = ''
    for items in metric_list:
        param_metric =  items + ',' + param_metric
    url = 'https://graph.facebook.com/v8.0/' + instagram_business_account +  '/insights' 
    payload = {
        'access_token': access_token,
        'limit' : 100,
        'period' : 'day',
         'metric': param_metric
        }
    
    logging.info (" GET request url : '{0}' and parameters : '{1}'".format(url,payload))
    response = requests.get(url, params=payload)
    print(json.dumps(response.json(),indent=4))

    return instagram_business_account, response.status_code, response.json()




def instgm_account_insights_create_csv(bucket_name, object_key):

    #Inputs: bucketname of the source file that needs to be converted.
    #        object key of the source file that needs to be converted. 
    
    logging.info("Function instgm_account_insights_create_csv() invoked")
    file_name = object_key.split('/')[-1].split('.')[0]
    inobjectKey = object_key
    outobject_key =  'Instagram/Csv_Store/' + file_name + '.csv'
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
            values = metric['values'][1]['value']
            endtime = (metric['values'][1]['end_time'])[:10]
            csv_file.writerow([Id,Metric,values,endtime])

    logging.info("Upload params : csv file name {0} uploaded object {1}".format(csv_file_name, outobject_key))

    try :
        s3.upload_file(csv_file_name,bucket_name,outobject_key)
        logging.info("File has been uploaded")
    except ClientError as e:
        logging.error(e)
        return False

    return True




def main():

    bucketName = 'rs.webanalytics'
    startdate = date.today()
    unixdate = int(startdate.strftime("%s"))
    filedate = date.today().strftime("%Y%m%d")
    instagram_business_account, status_code, response_json = get_instgm_account_insights('671278940149620','EAAIsFLlKv6wBAH907YhH6JqlY0numf3AWB4ZBZADtyhKRcwX4p3ynk25eiQJcl2YlSBRubK4LB5RfR0RXsK6lVm95pMDRz0AmWYd3pZC4jvPcw9CYQciZCJyt0AjZC0do5TrRZAMGl35yVhGroXHmlTj0ZBdESlQDW4xQVE755ySAZDZD',unixdate,unixdate)

    if (status_code == 200):
        filename = 'AccountInsights_' + instagram_business_account + '_' + filedate + '.json'
        ## Write raw response to S3
        outobjectKey = 'Instagram/Raw_Store/'+ filename
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
        exit(0)

     ## Convert JSON to CSV and move to CSV folder
    status = instgm_account_insights_create_csv(bucketName, outobjectKey)
    if ( status == True ):
        logging.info("Conversion to csv successful ")
    else :
        logging.info("Conversion to csv failed ")
        exit(0)

if __name__ == "__main__":
    main()
