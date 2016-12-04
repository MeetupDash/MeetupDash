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

client = MongoClient("mongodb://152.46.19.205:27017")
db = client['meetup']
collection = db['cityEvent']

r = redis.StrictRedis(host='152.46.19.205', port=6379, db=10)

def createDict(values):
	temp_dict = {}
	for value in values:
		if value in temp_dict:
			temp_dict[value] = temp_dict[value] + 1
		else:
			temp_dict[value] = 1
	return temp_dict

def flatten_dict(values):
	temp_dict = {}

	for dictionary in values:
		for key, value in dictionary.iteritems():
			if key in temp_dict:
				temp_dict[key] = temp_dict[key] + value
			else:
				temp_dict[key] = value

	return temp_dict

def flatten(values):
	temp_list = []
	for value in values:
		for word in value:
			temp_list.append(word)
	return temp_list

def extractWords(event):
	temp_list = []
	city = event[0]
	for keyword in event[1]:
		words = (re.split(r'\s{1,}', keyword[0]))
		for word in words:
			if word.isalnum() and len(word)>2:
				temp_list.append(word)
	return (city,temp_list)

def createSortedList(event):
	return sorted(event, key=event.get, reverse=True)

def addWeightEventName(record):
	new_dict = {}
	for event, count in record[1].iteritems():
		new_dict[event] = count*2

	return (record[0], new_dict)

def addWeightGroupName(record):
	new_dict = {}
	for event, count in record[1].iteritems():
		new_dict[event] = count*1.5

	return (record[0], new_dict)



#sc = SparkContext('local','example')

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
# event_df = event_df.select(concat(col("event_name"), lit(" "), col("description"), lit(" "), col("group_name")).alias("data"), col("city"))


Rake = RAKE.Rake("/home/rahuja/RakeTest/SmartStoplist.txt")


event_name_df = event_df.select(col("event_name").alias("data"), col("city"))
event_rdd_event_name = event_name_df.rdd.map(lambda x: (x.city, Rake.run(x.data)))
event_rdd_event_name_1 = event_rdd_event_name.map(extractWords)
event_rdd_event_name_2 = event_rdd_event_name_1.groupByKey().mapValues(list).mapValues(flatten)
event_rdd_event_name_3 = event_rdd_event_name_2.mapValues(createDict)
event_rdd_event_name_4 = event_rdd_event_name_3.map(addWeightEventName)

event_group_df = event_df.select(col("group_name").alias("data"), col("city"))
event_rdd_group_name = event_group_df.rdd.map(lambda x: (x.city, Rake.run(x.data)))
event_rdd_group_name_1 = event_rdd_group_name.map(extractWords)
event_rdd_group_name_2 = event_rdd_group_name_1.groupByKey().mapValues(list).mapValues(flatten)
event_rdd_group_name_3 = event_rdd_group_name_2.mapValues(createDict)
event_rdd_group_name_4 = event_rdd_group_name_3.map(addWeightGroupName)

event_desc_df = event_df.select(col("description").alias("data"), col("city"))
event_rdd_description = event_desc_df.rdd.map(lambda x: (x.city, Rake.run(x.data)[0:5]))
event_rdd_description_1 = event_rdd_description.map(extractWords)
event_rdd_description_2 = event_rdd_description_1.groupByKey().mapValues(list).mapValues(flatten)
event_rdd_description_3 = event_rdd_description_2.mapValues(createDict)

event_rdd = event_rdd_event_name_4.union(event_rdd_group_name_4).union(event_rdd_description_3)
event_rdd_1 = event_rdd.groupByKey().mapValues(list).mapValues(flatten_dict)
event_rdd_2 = event_rdd_1.mapValues(createDict)
event_rdd_3 = event_rdd_2.mapValues(createSortedList)

event_data = event_rdd_3.collect()

for event in event_data:
	event_object = {"city" : event[0], "events": event[1]}
	collection.insert_one(event_object).inserted_id

for event in event_data:
	for item in event[1]:
		r.rpush(event[0], item)
