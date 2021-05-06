# wallstreetbets-sentiment-analysis
## The Project aims at collecting the daily posts from Reddit's r/wallstreetbets daily discussion thread and displaying some analytical results as an interactive dashboard.

About the data collection and the app:
A free-tire **AWS EC2 instance** is being used to collect data using **pushshift** and **Praw**. Pushshift allows you to fetch reddit posts based on datetime(Praw doesn't), besides, Praw is being used to collect the comments. So, I first saved the data collected as compressed CSV's into **S3 buckets**, let's call this the **ELT approach** as I wasn't sure what I'd like to derive out of the data exactly, so loading the data as it is into S3 would allow me to be flexible with my research, I believe ETL makes more sense when we know exactly what we want to work on. The data pipeline finds the last file upload date on S3 and fetches the reddit data from that following day keeping the continuity of the data being collected. Files from the S3 bucket are used to populate a table which was already created on **Posgresql**(the **AWS RDS** services are used to achieve this). A **streamlit app** is created for visualizing the data being collected. Further upgrates to the are on the way ;)

fetch_reddit_data.py is run on a daily basis by setting up a **cron job**. Use the "crontab -e" on the terminal and add, for example, the following line towards the end of the file to run the cronjob on your instance, don't forget to change the file locations accordingly and also make sure to create crontest.log using "touch crontest.log" on the terminal while in the project folder/anywhere else you prefer. Generally logs are not in the project folder.
Eg: 0 5 * * *  /home/ubuntu/wallstreetbets-sentiment-analysis/runcron-env.py>> /home/ubuntu/wallstreetbets-sentiment-analysis/crontest.log 2>&1
Check out this cool website to experiment your cron schedule expressions https://crontab.guru/, mine is setup to run at 5AM everyday.

database-wsb.py has some examples on how you could interact with postgres DB

test_data.csv, I also downloaded the data collected over the last few months into a csv file if someone would like to use it directly!


## To run the script on your local machine/EC2
Pull the project from github and ideally, you should create and activate a new virtual environment before installing the necessary modules and running the script to avoid messing up the OS.
link on how to use virtual environments on ubuntu: https://stackoverflow.com/a/35017905/4197386

Firstly install the modules with the help of requirements.txt file by using the following command:
#### "pip install -r requirements.txt"
To run the script, use the following command: (check out the comment under call_api() in app.py to use the csv file directly instead of pulling data from the DB)
#### "streamlit run app.py"

