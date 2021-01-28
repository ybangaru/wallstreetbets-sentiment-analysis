import requests
import datetime
import json
import time
import pandas as pd
import os
import praw
import datetime
import boto3
from botocore.exceptions import ClientError

# from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

def get_submissions(from_date, to_date, sub, title_with):
    base_url = 'https://api.pushshift.io/reddit/search/submission/'
    url = f'{base_url}?subreddit={sub}&title={title_with}&after={from_date}&before={to_date}'
    data = requests.get(url)
    data = json.loads(data.text)
    return data['data']

def get_reddit_submissions(from_date, to_date, sub):
    title_with = 'Daily Discussion Thread'
    subStats = []
    all_submissions = get_submissions(from_date, to_date, sub, title_with)

    while len(all_submissions)>0:
        for submission in all_submissions:
            subData = [submission['id'], submission['title'], submission['url'], datetime.datetime.fromtimestamp(submission['created_utc'])]
            try:
                flair = submission['link_flair_text']
            except KeyError:
                flair = "NaN"
            subData.append(flair)
            subStats.append(subData)

        from_date = all_submissions[-1]['created_utc']
        all_submissions = get_submissions(from_date, to_date, sub, title_with)

    columns = ['id', 'title', 'url', 'date', 'flair']
    all_submissions = pd.DataFrame(subStats, columns=columns)
    # all_submissions.to_csv(f'{os.getcwd()}/reddit_data.csv')
    return all_submissions

def get_reddit_comments(all_submissions):
    reddit = praw.Reddit(client_id = 'mteYn8e2rsWy2g', client_secret='ZqDp8o_bY1Vv445VcNp6tWsZCsKGCA', user_agent='nlp')
    comments_by_day = []
    for url in all_submissions['url']:
        try:
            submission = reddit.submission(url=url)
            # print(submission.title)
            for comment in submission.comments:
                # print(comment.body)
                submission.comments.replace_more(limit=0)
                comments=list([(comment.body) for comment in submission.comments])
                # print(comments)
        except:
            comments=None
        comments_by_day.append(comments)
    return comments_by_day


def upload_file(s3_client, file_name, bucket, object_name=None):

    if object_name is None:
        object_name = file_name

    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def main(sub):

    creds = pd.read_csv('yashaccess.csv')
    AWS_S3_CREDS = {
    "aws_access_key_id": creds.columns[0], # os.getenv("AWS_ACCESS_KEY")
    "aws_secret_access_key":creds.columns[1] # os.getenv("AWS_SECRET_KEY")
    }
    s3_client = boto3.client('s3', **AWS_S3_CREDS)
    objects = s3_client.list_objects(Bucket='reddit-wallstreetbets')

    latest_obj = pd.DataFrame(objects['Contents']).sort_values(by='LastModified', ascending=True)['Key'].iloc[-1]
    print(latest_obj)

    resp = s3.select_object_content(
        Bucket='reddit-wallstreetbets',
        Key=latest_obj,
        ExpressionType='SQL',
        Expression="SELECT * FROM s3object s where s.\"Name\" = 'Jane'",
        InputSerialization = {'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'GZIP'},
        OutputSerialization = {'CSV': {}},
    )


    # from_date = time.mktime(time.strptime('2018-03-01', '%Y-%m-%d'))
    # to_date = time.mktime(time.strptime('2018-03-31', '%Y-%m-%d'))

    # all_submissions = get_reddit_submissions(int(from_date), int(to_date), sub)
    # all_comments = get_reddit_comments(all_submissions)
    # all_submissions['comments'] = all_comments
    # print(all_submissions['date'])
    # temporal = datetime.datetime.now().strftime("%Y-%m-%d-%I:%M:%S-%p")
    # filename = f'{sub}-{temporal}.csv.gz'
    # all_submissions.to_csv(f'{os.getcwd()}/{filename}',  compression='gzip', index=False)
    

    # if upload_file(s3_client, filename, 'reddit-wallstreetbets') is True:
    #     print('Upload successful')
    #     os.remove(f'{os.getcwd()}/{filename}')
    # else:
    #     print('S3 Uplaod failed')
    
    # # now append the all_submission along with their scores to the database again

if __name__=='__main__':
    sub = "wallstreetbets"
    main(sub)