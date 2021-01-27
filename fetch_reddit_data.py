import requests
import datetime
import json
import time
import pandas as pd
import os
import praw

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

def get_submissions(from_date, to_date, sub, title_with):
    base_url = 'https://api.pushshift.io/reddit/search/submission/'
    url = f'{base_url}?subreddit={sub}&title={title_with}&after={from_date}&before={to_date}'
    data = requests.get(url)
    data = json.loads(data.text)
    return data['data']

def get_reddit_submissions(from_date, to_date):
    sub = "wallstreetbets"
    title_with = 'Daily Discussion Thread'
    subStats = []
    all_submissions = get_submissions(from_date, to_date, sub, title_with)

    while len(all_submissions)>0:
        for submission in all_submissions:
            subData = [submission['id'], submission['title'], submission['url'], datetime.datetime.fromtimestamp(submission['created_utc']).date()]
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

def get_scores(all_comments):
    scores=[]
    for comments in all_comments:
        sentiment_score=0
        try:
            for comment in comments:
                sentiment_score=sentiment_score+analyser.polarity_scores(comment)['compound']
        except TypeError:
            sentiment_score=0
        
        scores.append(sentiment_score)
    return scores

def main():
    # find the last submission id from sql/any database and use the date from that for from_date
    # to date is always today
    from_date = int(time.mktime(time.strptime('2018-06-01', '%Y-%m-%d')))
    to_date = int(time.mktime(time.strptime('2020-12-31', '%Y-%m-%d'))) 
    # all_submissions = get_reddit_submissions(from_date, to_date)

    columns = ['id', 'title', 'url', 'date', 'flair']
    all_submissions = pd.read_csv(f'{os.getcwd()}/reddit_data.csv', names=columns)
    all_submissions = all_submissions[:5][1:]

    # verify if any duplicates in all_submission by verifying with date and submission id
    # drop those duplicates and call the following function
    all_comments = get_reddit_comments(all_submissions[:5])
    scores = get_scores(all_comments)
    all_submissions['scores'] = scores

    # now append the all_submission along with their scores to the database again

if __name__=='__main__':
    main()