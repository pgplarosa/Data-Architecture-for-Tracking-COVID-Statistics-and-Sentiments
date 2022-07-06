import snscrape.modules.twitter as sntwitter
import pandas as pd
from datetime import date, timedelta, datetime
import json
import boto3
import pickle
import io
import numpy as np
import boto3
import re 

# CONSTANTS
aws_access_key_id = 'AKIA57LY6XUDBMQHDRTQ'
aws_secret_access_key = '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'
aws_access_key_id = aws_access_key_id.strip()
aws_secret_access_key = aws_secret_access_key.strip()
s3_bucket = 'de2022-final-project'
landing_path = 'landing/tweet-data/'
gold_path = 'gold/nosql/'

def scrape_tweets():
    # Creating list to append tweet data to
    current_date = datetime.now() - timedelta(1)
    next_date = datetime.now() 

    s3_client = boto3.resource('s3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key)

    current_str = current_date.strftime("%Y-%m-%d")
    next_str = next_date.strftime("%Y-%m-%d")
    # Using TwitterSearchScraper to scrape data and append tweets to list
    tweets_list = [] 


    query = sntwitter.TwitterSearchScraper(f'#covid19usa since:{current_str} until:{next_str} lang:en')

    for i,tweet in enumerate(query.get_items()):
        tweets_list.append(tweet.json())
        tweets_list = list(set(tweets_list))

    s3object = s3_client.Object(s3_bucket, f'{landing_path}{current_str}.json')                      
    s3object.put(
        Body=(bytes('\n'.join(tweets_list).encode('UTF-8')))
    )

    current_date = next_date
    next_date = next_date + timedelta(1)

#         if current_str == '2022-06-15':
#             break
    
def insert_to_dynamo():
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(s3_bucket)

    s3_client = boto3.client('s3', 
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key)

    comprehend = boto3.client('comprehend',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key)

    

    for my_bucket_object in my_bucket.objects.all():
        if re.match(f'{landing_path}{datetime.now().strftime("%Y-%m-%d")}.json', 
                    my_bucket_object.key):

            s3_response_object = s3_client.get_object(Bucket=s3_bucket, 
                                          Key=my_bucket_object.key)

            object_content = s3_response_object['Body']
            df = pd.read_json(object_content, lines=True)
            df['date'] = df.date.dt.strftime("%Y-%m-%d")
            df_tweets = df

            client = boto3.client('dynamodb')

            # replace nan to None for easier processing
            df_tweets = df_tweets.replace(np.nan, None, regex=False)
            # remove links
            df_tweets['content'] = df_tweets.content.str.replace(r'http.*?\b', '', regex=True)

            # remove hashtags; should be captured by hashtag feature
            df_tweets['content'] = df_tweets.content.str.replace(r'#.*?\b', '', regex=True)

            # remove non-alphaneumeric for compatability with dynamodb
            df_tweets['content'] = df_tweets.content.str.replace(r'[^A-Za-z0-9 ]+', '', regex=True)
            df_tweets['tokenized_content'] = df_tweets.content.str.split()

            print('start sentiment analysis... ')
            df_sentiment = pd.json_normalize(df_tweets.content.apply(lambda x: 
                                                      comprehend.detect_sentiment(Text=x,
                                                                                  LanguageCode='en')))

            print('end sentiment analysis... ')

            df_dynamo = pd.concat([df_tweets.reset_index(drop=True), 
                                   df_sentiment], axis=1)

            # get only relevant features
            df_dynamo = df_dynamo.loc[:, ['id', 'date', 'content', 'replyCount', 
                              'retweetCount', 'likeCount', 'quoteCount',
                              'hashtags', 'Sentiment', 'SentimentScore.Positive',
                               'SentimentScore.Negative', 'SentimentScore.Neutral',
                               'SentimentScore.Mixed', 'tokenized_content'
                          ]].drop_duplicates(subset=['id', 'date']).to_dict(orient='records')

            print('start insert dynamodb... ')
            for row in df_dynamo:
                try:
                    client.execute_statement(
                        Statement=f"""
                        INSERT INTO covid_tweets VALUE {str(row)}
                        """
                    )
                except:
                    continue

            print('end insert dynamodb... ')    

def dynamo_to_s3():
    client = boto3.client('dynamodb')

    output = client.execute_statement(
                    Statement="""
                    SELECT * FROM covid_tweets
                    """
                )['Items']

    s3_client = boto3.resource('s3',
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key)
    s3object = s3_client.Object(s3_bucket, gold_path + 'tweets_db.json')                      
    s3object.put(
        Body=(bytes('\n'.join([json.dumps(out) for out in output]).encode('UTF-8')))
    )

