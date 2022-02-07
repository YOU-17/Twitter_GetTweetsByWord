import urllib3
import config
import json
import time
import datetime

import pandas as pd

http = urllib3.PoolManager()
KEY = config.BEARER_TOKEN

# param1に該当するユーザーIDを取得
def getUserId(http, key, searchField):
    url = 'https://api.twitter.com/2/tweets/search/recent'
    req = http.request(
        'GET',
        url,
        headers = {'Authorization': 'Bearer '+key},
        fields = searchField
    )
    
    result = json.loads(req.data)
    if req.status == 200:
        if 'data' in result:
            lst = []
            for user in result['data']:
                lst.append(user['author_id'])
            return lst
    else:
        print(req.status)
        print(result['errors'])

# param2に該当するツイートを取得
def getTweetInfo(http, key, searchField):
    url = 'https://api.twitter.com/2/tweets/search/recent'
    req = http.request(
        'GET',
        url,
        headers = {'Authorization': 'Bearer '+key},
        fields = searchField
    )
    
    result = json.loads(req.data)
    if req.status == 200:
        if 'includes' in result:
            lstUI = []
            for user in result['includes']['users']:
                dctUI = {}
                dctUI['name'] = user['name']
                dctUI['username'] = user['username']
                dctUI['description'] = user['description']
                lstUI.append(dctUI)
                
        if 'data' in result:
            lstTI = []
            for tweet in result['data']:
                dctTI = {}
                dctTI['created_at'] = tweet['created_at']
                dctTI['text'] = tweet['text']
                dctTI['retweet_count'] = tweet['public_metrics']['retweet_count']
                dctTI['reply_count'] = tweet['public_metrics']['reply_count']
                dctTI['like_count'] = tweet['public_metrics']['like_count']
                dctTI['quote_count'] = tweet['public_metrics']['quote_count']
                lstTI.append(dctTI)

        return lstUI, lstTI
    elif req.status in (420, 429):
        print('now waiting')
        time.sleep(900)
    else:
        print(req.status)
        print(result['errors'])
        
        
        
# 取得するユーザー情報の設定
params1 = {
    'query': config.SERCH_WORD,
    'max_results':config.COUNT_USERS,
    'expansions': 'author_id'
          }



# 処理開始
lstIDs = getUserId(http, KEY, params1)

lstUsersInfo = []
lstTweetsInfo = []
for userID in lstIDs:
    lstInfo = []
    authorID = userID
    # 取得するツイート情報の設定
    params2 = {
        'query': 'from:{}'.format(authorID),
        'max_results': config.COUNT_TWEETS_EACH,
        'expansions': 'author_id,attachments.media_keys',
        'media.fields' : 'preview_image_url,type',
        'tweet.fields': 'created_at,public_metrics',
        'user.fields'  : 'description,name'
            }
    
    results = getTweetInfo(http, KEY, params2)
    lstUsersInfo += results[0]
    lstTweetsInfo += results[1]
    
    
    
# date取得
t_delta = datetime.timedelta(hours = 9)
JST = datetime.timezone(t_delta, 'JST')
now = datetime.datetime.now(JST)
d = now.strftime('%Y%m%d%H%M')

# csv出力
dfUI = pd.DataFrame(lstUsersInfo)
dfUI.to_csv('UsersInfo_{}.csv'.format(d))

dfTI = pd.DataFrame(lstTweetsInfo)
dfTI.to_csv('TweetsInfo_{}.csv'.format(d))