#####
# Read data from Facebook Graph API
####

import json
import facebook
from  utility import * 
import pandas as pd

def get_posts():

    page_id,access_token = refresh_fb_access_token()
    
    graph =  facebook.GraphAPI(access_token)
    
    posts = graph.get_object(id = page_id ,fields = 'posts')

    print(json.dumps(posts, indent=4))

    print('Page id  = ', posts['id'] + '\n\n')
    for items in posts['posts']['data']:
        print(items)

              
def main():
    get_posts()

if __name__ == "__main__":
    main()
