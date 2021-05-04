# import psycopg2
import sys
import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, MetaData, Table, insert, delete
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, DateTime, String, Integer, func 
import boto3
import gzip
import psycopg2

creds = pd.read_csv(f'{os.getcwd()}/yashaccess.csv')
ENDPOINT="wallstreetbets-dd.c9f0d1vazbt9.us-west-2.rds.amazonaws.com"
PORT="5432"
USR="postgres"
token = creds.columns[4]
REGION="us-west-2a"
DBNAME = 'postgres'
DATABASE_URI = f'postgresql+psycopg2://{USR}:{token}@{ENDPOINT}:{PORT}/{DBNAME}'
print(token)

# Base = declarative_base()

# class Submission(Base):  
#     __tablename__ = 'daily-submissions-wsb'

#     id = Column(String, primary_key=True)
#     title = Column(String)
#     url = Column(String)
#     datetime = Column(String)
#     flair = Column(String)
#     comments=Column(String)
#     when_created = Column(DateTime, default=func.now())

#     def repr(self):
#         print(f'id:{self.id}, title:{self.title}, url={self.url}, datetime={datetime}, created={when_created}')
# Base.metadata.create_all(bind=engine)


engine = create_engine(DATABASE_URI, echo=False)
connection = engine.connect()

# # Names of tables
# print(engine.table_names())

# # Query all the items of the table
# stmt = 'SELECT * FROM "daily-submissions-wsb"'
# result_proxy = connection.execute(stmt)
# results = result_proxy.fetchall()
# print(results)
# ids_list = [ item[0] for item in results]
# print(len(results))

# # # Query specific columns
# stmt = 'SELECT datetime FROM "daily-submissions-wsb"'
# result_proxy = connection.execute(stmt)
# results = result_proxy.fetchall()
# print(results[-1][0])


# # # Query last n items of the table
stmt = 'SELECT * FROM "daily-submissions-wsb" ORDER BY datetime DESC'
result_proxy = connection.execute(stmt)
results = pd.DataFrame(result_proxy.fetchall(), columns=['id', 'title', 'url', 'datetime', 'comments', 'flair'])
# print(results['comments'])
# results.to_csv('test_data.csv', index=False)
print(results)


# engine = create_engine(DATABASE_URI, echo=False)
# connection = engine.connect()
# print(engine.table_names())
# stmt = 'SELECT id FROM "daily-submissions-wsb"'
# result_proxy = connection.execute(stmt)
# results = result_proxy.fetchall()
# ids_list = [ item[0] for item in results]
# print(ids_list)

# metadata = MetaData()
# daily_subs = Table('daily-submissions-wsb', metadata, autoload=True, autoload_with=engine)

# AWS_S3_CREDS = {
# "aws_access_key_id": creds.columns[0], # os.getenv("AWS_ACCESS_KEY")
# "aws_secret_access_key":creds.columns[1] # os.getenv("AWS_SECRET_KEY")
# }
# bucket = 'reddit-wallstreetbets'
# s3_client = boto3.client('s3', **AWS_S3_CREDS)
# objects = s3_client.list_objects(Bucket=bucket)

# all_objects = objects['Contents']

# csv_rows = []
# for obj in all_objects:
#     obj = s3_client.get_object(Bucket=bucket, Key=obj['Key'])['Body']

#     with gzip.open(obj, 'rt') as gf:
#         df = pd.read_csv(gf)

#         for index,row in df.iterrows():
#             if row["id"] in ids_list:
#                 connection.execute(delete(daily_subs).where(daily_subs.columns.id==row["id"]))
#                 print(f"deleted a row with id {row['id']}")
#             try:
#                 comments = row["comments"][1:-2]
#             except:
#                 comments = np.nan
#             table_values={
#                 'id'      : row["id"],
#                 'title'   : row["title"],
#                 'url'     : row["url"],
#                 'datetime' : row["date"],
#                 'flair'   : row["flair"],
#                 'comments' : comments,
#                 #    'when_created' : Column(DateTime, default=func.now())
#             }
#             csv_rows.append(table_values)

# stmt = insert(daily_subs)
# result_proxy = connection.execute(stmt, csv_rows)
# print(result_proxy.rowcount)
