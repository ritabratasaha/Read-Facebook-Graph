import json
import requests
import logging

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
