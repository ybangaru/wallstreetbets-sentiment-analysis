import requests
import datetime
import json
import time
import pandas as pd
import os
import praw
import datetime
from dateutil.relativedelta import *
import boto3
from botocore.exceptions import ClientError


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
    creds = pd.read_csv(f'{os.getcwd()}/yashaccess.csv')
    reddit = praw.Reddit(client_id = creds.columns[2], client_secret=creds.columns[3], user_agent='nlp')
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

    creds = pd.read_csv(f'{os.getcwd()}/yashaccess.csv')
    AWS_S3_CREDS = {
    "aws_access_key_id": creds.columns[0], # os.getenv("AWS_ACCESS_KEY")
    "aws_secret_access_key":creds.columns[1] # os.getenv("AWS_SECRET_KEY")
    }
    s3_client = boto3.client('s3', **AWS_S3_CREDS)
    objects = s3_client.list_objects(Bucket='reddit-wallstreetbets')

    latest_obj = pd.DataFrame(objects['Contents']).sort_values(by='LastModified', ascending=True)['Key'].iloc[-1]

    resp = s3_client.select_object_content(
        Bucket='reddit-wallstreetbets',
        Key=latest_obj,
        ExpressionType='SQL',
        Expression='SELECT s."date" FROM S3Object s',
        InputSerialization = {'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'GZIP'},
        OutputSerialization = {'CSV': {}},
    )

    for event in resp['Payload']:
        if 'Records' in event:
            last_date = sorted(event['Records']['Payload'].decode('utf-8').split('\n'))[-1]

    # print(last_date)
    #to fetch historical data on a monthly basis
    # to_date = datetime.datetime.strptime(last_date, '%Y-%m-%d %H:%M:%S') + relativedelta(months=+1)
    # to_date = time.mktime(time.strptime(str(to_date), '%Y-%m-%d %H:%M:%S'))   
    from_date = time.mktime(time.strptime(last_date,'%Y-%m-%d %H:%M:%S'))
    to_date = time.time()

    all_submissions = get_reddit_submissions(int(from_date), int(to_date), sub)
    all_comments = get_reddit_comments(all_submissions)
    all_submissions['comments'] = all_comments

    if all_submissions.empty:
        print('No data Collected!')
    else:
        temporal = datetime.datetime.now().strftime("%Y-%m-%d-%I:%M:%S-%p")
        filename = f'{sub}-{temporal}.csv.gz'
        all_submissions.to_csv(f'{os.getcwd()}/{filename}',  compression='gzip', index=False)
        

        if upload_file(s3_client, filename, 'reddit-wallstreetbets') is True:
            print(f'Upload successful beginning {last_date} for a month')
            os.remove(f'{os.getcwd()}/{filename}')
        else:
            print('S3 Upload failed')
    
if __name__=='__main__':
    sub = "wallstreetbets"
    main(sub)

