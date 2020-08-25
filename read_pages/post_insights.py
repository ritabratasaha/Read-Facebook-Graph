import json
import requests
import logging

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
    logging.info("Total Posts length from response : {0}".format(len(response.json()['data'])))
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
            print(post_id)
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

