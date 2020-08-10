#####
# Read data from Facebook Graph API
####

import json
import facebook
from  utility import get_accessToken 
import pandas as pd

def get_posts():

    print()
    graph =  facebook.GraphAPI(access_token=get_accessToken())
    
    # app_id = '611417473073068'
    # app_secret = '39b76b9499d3623d4a31b6ed9a200c7d'
    # graph = facebook.GraphAPI()
    # access_token = graph.get_app_access_token(app_id, app_secret)

    posts = graph.get_object(id = '671278940149620',fields = 'posts')

    print(json.dumps(posts, indent=4))

    print('Page id  = ', posts['id'] + '\n\n')
    for items in posts['posts']['data']:
        print(items)

              
def main():
    get_posts()

if __name__ == "__main__":
    main()
