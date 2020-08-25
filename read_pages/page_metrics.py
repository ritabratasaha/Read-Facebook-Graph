import json
import requests
import logging

def get_page_metrics(page_id,access_token):

    ## Pull page metrics
    metric_list = ['fan_count','engagement']
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