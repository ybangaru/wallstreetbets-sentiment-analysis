import streamlit as st
import numpy as np
import pandas as pd
import datetime
import os
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import plotly.graph_objects as go
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import plotly.express as px
from plotly.subplots import make_subplots
import seaborn as sns

import nltk
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer 
from nltk import ngrams

from collections import Counter

@st.cache
def run_app():

    def api_data():
        ENDPOINT = st.secrets["db_ENDPOINT"]
        PORT = 5432
        USR = st.secrets["db_usr"]
        token = st.secrets["db_token"]
        REGION= st.secrets["db_region"]
        DBNAME = st.secrets["db_name"]
        DATABASE_URI = f'postgresql+psycopg2://{USR}:{token}@{ENDPOINT}:{PORT}/{DBNAME}'

        engine = create_engine(DATABASE_URI, echo=False)
        connection = engine.connect()
        stmt = 'SELECT * FROM "daily-submissions-wsb"'
        result_proxy = connection.execute(stmt)
        results = result_proxy.fetchall()
        return results

    def call_api(no_days):
        """
        Comment out the first 2 lines and comment the 3rd line here to use the data
        from the csv file directly
        """
        # root = f"{os.getcwd()}"
        # df = pd.read_csv(f'{root}/test_data.csv')
        df = pd.DataFrame(api_data(), columns=['id', 'title', 'url', 'datetime', 'comments', 'flair'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df.sort_values(by='datetime', ignore_index=True)
        df = df.reset_index(drop=True)
        df = df[-no_days:]
        # print(df)
        return df
    
    stop_words = set(STOPWORDS)
    def show_wordcloud(data, title = None):
        """Word cloud image"""
        wordcloud = WordCloud(
            background_color='black',
            stopwords=stop_words,
            max_words=200,
            max_font_size=40, 
            scale=3,
            random_state=1 # chosen at random by flipping a coin; it was heads
        ).generate(str(data))

        fig = plt.figure(1, figsize=(15, 15))
        plt.axis('off')
        if title: 
            fig.suptitle(title, fontsize=20)
            fig.subplots_adjust(top=2.3)

        plt.imshow(wordcloud)

        return fig

    def get_vader_sentiment(comments_by_day):
        analyser = SentimentIntensityAnalyzer()
        scores=[]
        for comments in comments_by_day:
            sentiment_score=0
            try:
                sentiment_score=analyser.polarity_scores(comments)['compound']
            except TypeError:
                sentiment_score=0
                        
            scores.append(sentiment_score)
            
        return scores

    def clean_words(new_tokens):
        new_tokens = [t.lower() for t in new_tokens]
        new_tokens =[t for t in new_tokens if t not in stopwords.words('english')]
        new_tokens = [t for t in new_tokens if t.isalpha()]
        lemmatizer = WordNetLemmatizer()
        new_tokens =[lemmatizer.lemmatize(t) for t in new_tokens]
        return new_tokens        

    def get_all_clean_words_plot(all_data):
        fig, ax = plt.subplots()
        ax = sns.barplot(x='frequency',y='words',data=all_data)
        return fig

    def get_sentiment_plot(cleaned_data):
        fig, ax = plt.subplots()
        ax = sns.lineplot(data=cleaned_data, x="datetime", y="sentiment_scores")
        plt.xticks(rotation=45)
        return fig

    st.title("Some basic charts!!")
    no_days = st.number_input("How many days into the past would you like to look into!!", min_value=7, step=7, max_value=28)
    cleaned_data = call_api(no_days)

    final_string = ""
    for day in range(len(cleaned_data['comments'])):
        final_string+=cleaned_data['comments'].iloc[-day]
    st.title(f"Word Cloud for the last {no_days} days")
    st.pyplot(show_wordcloud(final_string))

    # word_counts_df = (cleaned_data['comments'].str.split(expand=True).stack().value_counts().rename_axis('vals').reset_index(name='count'))
    # word_counts_df = word_counts_df[:30]
    # print(word_counts_df)

    cleaned_data['words'] = cleaned_data['comments'].apply(lambda x : word_tokenize(x))
    cleaned_data['clean_words'] = cleaned_data['words'].apply(lambda x:clean_words(x))
    all_clean_words = pd.DataFrame(Counter(cleaned_data['clean_words'].sum()).items(), columns=['words','frequency']).sort_values(by='frequency',ascending=False).head(30)
    # print(all_clean_words)
    st.title(f"Most Frequently used words!")
    st.pyplot(get_all_clean_words_plot(all_clean_words))

    st.title(f"Top Bigrams with their frequency")
    two_words_df = pd.DataFrame(Counter(ngrams(cleaned_data['clean_words'].sum(),2)).items(), columns=['words','frequency']).sort_values(by='frequency',ascending=False).head(30)
    # print(two_words_df)
    st.pyplot(get_all_clean_words_plot(two_words_df))

    st.title(f"Top Trigrams with their frequency")
    three_words_df = pd.DataFrame(Counter(ngrams(cleaned_data['clean_words'].sum(),3)).items(), columns=['words','frequency']).sort_values(by='frequency',ascending=False).head(30)
    # print(three_words_df)
    st.pyplot(get_all_clean_words_plot(three_words_df))

    cleaned_data['sentiment_scores'] = get_vader_sentiment(cleaned_data['comments'])
    st.title(f"Vader Sentiment scores for the last {no_days} days")
    st.pyplot(get_sentiment_plot(cleaned_data))



def main():
    st.sidebar.markdown("""
        <h2>Choose the mode:</h2>    
    """, unsafe_allow_html=True)
    mode = st.sidebar.selectbox('', [
        'Dashboard!!',
        'About the Project!',
    ])
    if mode == 'About the Project!':
        st.title('Details of the project!')
        st.markdown("""
                check out my github @ https://github.com/ybangaru/wallstreetbets-sentiment-analysis
            """)

    elif mode == 'Dashboard!!':
        st.title('Wall Street Bets Daily Discussion Thread Analysis')
        st.markdown("""
            Wall Street Bets is a subreddit with 1.8 million members (they call themselves "degenerates"), lol!! that's funny tbh.
            Anyways the project idea is to follow the recent sentiment on the daily discussion thread which is the most discussed thread
            everyday on this subreddit. It's safe enough to say it's the most discussed financial thread on a daily basis in the world.
            I mean, if we can't pickup sentiment of the market here, I'd expect, it'd be quite difficult to do that anywhere else ¯\\\_(ツ)_/¯  Change my mind! ;)
            Work still in progress!
            """)
        st.write("""The recent GME spike was so fun to follow really! Here, have a look as these posts: 
            https://www.theverge.com/2021/2/24/22299795/gamestop-stock-up-reddit-wallstreetbets-gme-pump & 
            https://www.bloomberg.com/news/articles/2021-01-25/how-wallstreetbets-pushed-gamestop-shares-to-the-moon""")
        run_app()


if __name__ == '__main__':
    main()        