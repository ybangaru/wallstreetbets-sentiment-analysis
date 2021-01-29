#!/bin/bash

source /home/ubuntu/.virtualenvs/sentiment/bin/activate
cd wallstreetbets-sentiment-analysis/
python /home/ubuntu/wallstreetbets-sentiment-analysis/fetch_reddit_data.py
