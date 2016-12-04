from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat
from pymongo import MongoClient
import redis
import pandas as pd
import re
import numpy as np
import RAKE

r = redis.StrictRedis(host='152.46.19.205', port=6379, db=7)

def extractWords(event):
	temp_list = []
	city = event[0]
	for keyword in event[1]:
		words = (re.split(r'\s{1,}', keyword[0]))
		for word in words:
			temp_list.append(word)
	return (city,temp_list)

def countCities(cities):
	sorted_list = []
	counter_cities = Counter(cities)
	for city, count in counter_cities.most_common():
		sorted_list.append(city)

	return sorted_list

def flatten(values):
	temp_list = []
	for value in values:
		for word in value:
			temp_list.append(word)
	return temp_list

def createDict(values):
	temp_dict = {}
	for value in values:
		if value in temp_dict:
			temp_dict[value] = temp_dict[value] + 1
		else:
			temp_dict[value] = 1
	return temp_dict

def createSortedList(event):
	return sorted(event, key=event.get, reverse=True)

conf = SparkConf()
conf.setMaster('spark://152.46.16.246:7077')
conf.setAppName('spark-basic')
sc = SparkContext(conf=conf)

spark = SparkSession.builder.appName("Keyword Extraction").getOrCreate()
event_df = spark.read.format('com.databricks.spark.csv').options(header='true').load('hdfs://152.46.16.246/user/rahuja/In/test.csv')
sql_sc = SQLContext(sc)

# raw_df = pd.read_csv('/home/rohit910/CSC591-DIC/output_nocomma1.csv')
# raw_df = pd.read_csv('hdfs://152.46.16.246/user/rahuja/In/output_nocomma1.csv')

#raw_df = pd.read_csv('/home/rahuja/output_final.csv')

#raw_df_1 = raw_df.replace(np.nan,' ', regex=True)
#raw_df_2 = raw_df_1[['event_id','event_name','description','city','group_name']]
#event_df = sql_sc.createDataFrame(raw_df_2)

event_df = event_df.select(concat(col("event_name"), lit(" "), col("description"), lit(" "), col("group_name")).alias("data"), col("city"))

#Rake = RAKE.Rake("/home/rohit910/CSC591-DIC/python-rake/stoplists/SmartStoplist.txt")

Rake = RAKE.Rake("/home/rahuja/RakeTest/SmartStoplist.txt")

event_rdd = event_df.rdd.map(lambda x: (x.city, Rake.run(x.data)[0:5])).map(extractWords).flatMapValues(lambda x:  x).map(lambda x: (x[1],x[0])).groupByKey().mapValues(list).mapValues(createDict).mapValues(createSortedList)
# event_rdd_1 = event_rdd.map(extractWords)
# event_rdd_2 = event_rdd_1.flatMapValues(lambda x:  x)
# event_rdd_3 = event_rdd_2.map(lambda x: (x[1],x[0]))
# event_rdd_4 = event_rdd_3.groupByKey().mapValues(list)

# event_rdd_5 = event_rdd_4.mapValues(createDict)
# event_rdd_6 = event_rdd_5.mapValues(createSortedList)

city_data = event_rdd.collect()

for event in city_data:
	for city in event[1]:
		r.rpush(event[0], city)