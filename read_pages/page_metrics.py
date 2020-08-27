import json
import requests
import logging
import boto3
from  utility import refresh_fb_access_token,set_session,get_fb_page_current_accessToken
import csv

def get_page_metrics(page_id,access_token):

    ## Pull page metrics
    metric_list = ['fan_count','engagement','location{city,country}']
    param_metric = ''
    for items in metric_list:
        param_metric =  items + ',' + param_metric
    param_metric = param_metric.rstrip(',')

    url = 'https://graph.facebook.com/v8.0/' + '671278940149620'       
    payload = {
        'access_token': access_token,
        'fields': param_metric
        }
    logging.info (" GET request url : '{0}' and parameters : '{1}'".format(url,payload))
    response = requests.get(url, params=payload)

    return response.status_code, response.json()




def page_metric_create_csv(bucket_name, object_key):

    #Inputs: bucketname of the source file that needs to be converted.
    #        object key of the source file that needs to be converted. 
        
    logging.info("Function page_metric_create_csv() invoked")
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

    id = access_dict['id']
    metric_value = []
    metric_value.append( tuple((id,'engagement',access_dict['engagement']['count'])) )
    metric_value.append( tuple((id,'fan_count',access_dict['fan_count'] )) )
    metric_value.append( tuple((id,'city',access_dict['location']['city'] )) )
    metric_value.append( tuple((id,'country',access_dict['location']['country'] )) )

    with open (csv_file_name,"w") as file:
        csv_file = csv.writer(file,quotechar='"',quoting=csv.QUOTE_ALL)
        csv_file.writerow(["Id","Metric","Value"])
        for each in metric_value:
            csv_file.writerow(  [each[0],each[1],each[2]] )

    logging.info("Upload params : csv file name {0} uploaded object {1}".format(csv_file_name, outobject_key))

    try :
        s3.upload_file(csv_file_name,bucket_name,outobject_key)
        logging.info("File has been uploaded")
    except ClientError as e:
        logging.error(e)
        return False

    return True