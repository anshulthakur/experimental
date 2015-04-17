from twitter import *
try:
    from access_token import * #Put your tokens in this file
except ImportError:
    ACCESS_TOKEN=''
    ACCESS_PASSWORD=''
    CONSUMER_KEY=''
    CONSUMER_SECRET=''

t = Twitter(auth=OAuth(ACCESS_TOKEN, #Access Token
                       ACCESS_PASSWORD, #Access Token Password
                       CONSUMER_KEY, #Consumer Key
                       CONSUMER_SECRET, #Consumer Key secret
                       )
            )

tweet_ids = []

screen_name = 'Blasphemous Aesthete'

def get_tweets():
    print 'getting tweets...'
    tweets = t.statuses.user_timeline(screen_name=screen_name, count=1)
    for tweet in tweets:
        print '\n',tweet['id'],'\t',tweet
        tweet_ids.append(tweet['id'])
    return True

def delete_tweet():
    print 'deleting tweets...'
    if len(tweet_ids) > 0:
        for id in tweet_ids:
            print id
            try:
                resp = t.status.destroy(id=id)
            except:
                pass
            del tweet_ids[tweet_ids.index(id)]
        return True
    else:
        return False

def send_tweet(message = ''):
    print 'Trying to send tweet...'
    if len(message) >0:
        ret = t.statuses.update(status=message)
        #print ret
    else:
        print 'Pass in something'
'''
busy=True
while True and busy:
    busy = get_tweets() and delete_tweets()
    if not busy:
        t.statuses.update(status="Deleting...done.")
        print "Done."
'''
'''
if get_tweets() is True:
    for tweet in tweet_ids:
        print tweet,"\n"
'''
import sys

message=str(sys.argv[1])
if len(message) < 140:
    send_tweet(message = message)
else:
    print 'Message too long!'
