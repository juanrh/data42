#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Based on storm.py module from https://github.com/nathanmarz/storm/blob/master/storm-core/src/multilang/py/storm.py, and the examples from https://github.com/apache/incubator-storm/blob/master/examples/storm-starter/multilang/resources/splitsentence.py and http://storm.incubator.apache.org/documentation/Tutorial.html

Packaging: To run a shell component on a cluster, the scripts that are shelled out to must be in the resources/ directory within the jar submitted to the master (https://github.com/nathanmarz/storm/wiki/Multilang-protocol). By default, Maven will look for your project's resources under src/main/resources (https://maven.apache.org/plugins/maven-resources-plugin/examples/resource-directory.html).

Tested in Python2.7 and Apache Storm 0.9.0.1
'''
import storm, tweepy
import json, time
from operator import itemgetter
import get_tweets

def log_tweeter_error(tweep_error, sleep_time=2):
    '''
    :param tweep_error: Exception dealing with twitter to log to the parent process
    :type api: tweepy.TweepError

    :param sleep_time: time in seconds to sleep before continuing the execution
    '''
    # We have hit the REST API Rate limit for Twitter https://dev.twitter.com/docs/rate-limiting/1.1, no more tweets for some time
    storm.log("Tweepy error {error}, sleeping for {secs} seconds in case Twitter rate limit has been hit".format(error=str(tweep_error), secs=sleep_time))
    time.sleep(sleep_time)

class PlacesSpout(storm.Spout):
    '''
    Emit a tuple with a single field for a place as encoded in the module get_tweets,
    with the frequency specified in the storm configuration passed at initialize()
    '''
    # Field in the configuration map where the emision frequency is 
    # configured
    _frequency_conf_key = "PlacesSpoutFrequency"
    def initialize(self, conf, context):
        # self._conf = conf
        # self._context = context
        self._places = get_tweets.available_places()
        self._tick_frequency = conf[self.__class__._frequency_conf_key]
    
    def nextTuple(self):
        '''
        Should adhere to the following contract expressed in the wrapping Java spout

        declarer.declare(new Fields(TopologyFields.PLACE));
        '''
        # storm.log(json.dumps({ "conf" : self._conf}))
        # storm.log(json.dumps({ "context" : self._context}))
        for place in self._places:
            storm.emit([place])
        time.sleep(self._tick_frequency)

class TwitterBolt(storm.Bolt):
    '''
    This class extends storm.Bolt as no ack is handled because we are using a non reliable source with no defined id for the messages.
    As additional functionality, a tweepy.api.API object ready to used is stored at self._twitter_api during initialization. 

    NOTE: don't forget to setup authentication calling "python2.7 get_tweets.py" __before__ compiling the topology: the auth file has to be included in the jar and copied to the cluster
    '''
    def initialize(self, stormconf, context):
        # Init connection to twitter API
        auth = get_tweets.authenticate(rebuild=False)
            # Better fail here if we cannot even authenticate
        self._twitter_api = tweepy.API(auth)

class TrendsBolt(TwitterBolt):
    '''
    Assumes each input tuple has a single field for the name of a place as encoded in get_tweets. This bolt emits tuples (place, trend name, query) for the trending topics at the coordinates corresponding to that place. 
    In case the Twitter REST API Rate limit is hit, this bolt sleeps for some seconds. 
    '''
    _rate_limit_sleep_time = 1

    def process(self, tuple):
        place = tuple.values[0]
        try: 
            trends = get_tweets.get_trending_topics_text(self._twitter_api, place)
        except tweepy.TweepError as te:
            # We have hit the REST API Rate limit for Twitter https://dev.twitter.com/docs/rate-limiting/1.1, no more tweets for some time
            log_tweeter_error(te, sleep_time=self._rate_limit_sleep_time)
            return 
        for trend in trends:
            storm.emit([place, trend['name'], trend['query']])

class GetTweetsBolt(TwitterBolt):
    '''
    Assumes each input tuple is of the shape (place, topic_name, query) where query is a twitter query string for the trending topic topic_name. For each input tuple Twitter is queried for the most popular tweets, and some fields are projected from each resulting tweets and then emitted, see process() for details about the fields. 

    In case the Twitter REST API Rate limit is hit, this bolt sleeps for some seconds. 
    '''
    @staticmethod
    def _storm_tweet_processor(status):
        shallow_fields = ['text', 'favorite_count', 'retweeted', 'in_reply_to_screen_name',
                          'retweet_count', 'possibly_sensitive', 'lang', 'created_at', 'source']
        ret = {k : status.__dict__.get(k, None) for k in shallow_fields}
        ret['created_at'] = ret['created_at'].strftime('%Y-%m-%d %H:%M:%S')
        ret['author_screen_name'] = status.author.screen_name
        ret['hashtags_texts'] = "|".join(sorted([hashtag['text'] for hashtag in status.entities['hashtags']]))
        ret['place_full_name'] = status.place.full_name if not status.place is None else None
        return ret

    _rate_limit_sleep_time = 1

    def process(self, tuple):
        '''
        Must fulfil the following contract expressed in the Java wrapper:

        declarer.declare(new Fields(TopologyFields.AUTHOR_SCREEN_NAME, TopologyFields.CREATED_AT,
                TopologyFields.FAV_COUNT, TopologyFields.HASHTAGS_TEXTS, TopologyFields.IN_REPLY_TO_SCREEN_NAME, 
                TopologyFields.LANG, TopologyFields.RETWEET_COUNT, TopologyFields.RETWEETED, 
                TopologyFields.SOURCE, TopologyFields.PLACE, TopologyFields.POSSIBLY_SENSITIVE,
                TopologyFields.TEXT, TopologyFields.TOPIC_NAME));
        '''
        place, topic_name, query = tuple.values
        try: 
            tweets = list(get_tweets.get_tweets_for_trends(self._twitter_api, [{"query" : query}], popular = True, tweet_processor = self._storm_tweet_processor))[0]["tweets"]
        except tweepy.TweepError as te:
            # We have hit the REST API Rate limit for Twitter https://dev.twitter.com/docs/rate-limiting/1.1, no more tweets for some time
            log_tweeter_error(te, sleep_time=self._rate_limit_sleep_time)
            return 

        for pt in tweets:
            # Here we add the trending topic name, and take the place name from those
            # used internally by get_tweets, instead of the from place names returned by twitter
            tup = [pt['author_screen_name'], pt['created_at'], 
                   pt['favorite_count'], pt['hashtags_texts'], pt['in_reply_to_screen_name'],
                   pt['lang'], pt['retweet_count'], pt['retweeted'],
                   pt['source'], place, pt['possibly_sensitive'],
                   pt['text'], topic_name]
            storm.emit(tup)