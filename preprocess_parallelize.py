from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, lit, concat
import pandas as pd
import re
import numpy as np
import RAKE

#sc = SparkContext('local','example')

conf = SparkConf()
conf.setMaster('spark://152.46.16.246:7077')
conf.setAppName('spark-basic')
sc = SparkContext(conf=conf)

spark = SparkSession.builder.appName("Keyword Extraction").getOrCreate()
event_df = spark.read.format('com.databricks.spark.csv').options(header='true').load('hdfs://152.46.16.246/user/rahuja/In/output_nocomma1.csv')
sql_sc = SQLContext(sc)

# raw_df = pd.read_csv('/home/rohit910/CSC591-DIC/output_final.csv')
# raw_df = pd.read_csv('hdfs://152.46.16.246/user/rahuja/In/output_nocomma1.csv')

#raw_df = pd.read_csv('/home/rahuja/output_final.csv')

#raw_df_1 = raw_df.replace(np.nan,' ', regex=True)
#raw_df_2 = raw_df_1[['event_id','event_name','description','city','group_name']]
#vent_df = sql_sc.createDataFrame(raw_df_2)

event_df = event_df.select(concat(col("event_name"), lit(" "), col("description"), lit(" "), col("group_name")).alias("data"), col("city"))

#Rake = RAKE.Rake("/home/rohit910/CSC591-DIC/python-rake/stoplists/SmartStoplist.txt")

Rake = RAKE.Rake("/home/rahuja/RakeTest/SmartStoplist.txt")


event_rdd = event_df.rdd.map(lambda x: (x.city, Rake.run(x.data)[0:5]))
event_rdd_1 = event_rdd.map(extractWords)
event_rdd_2 = event_rdd_1.groupByKey().mapValues(list).mapValues(flatten)
event_rdd_3 = event_rdd_2.mapValues(createDict);

event_rdd_4 = event_rdd_3.collect()

def createDict(values):
	temp_dict = {}
	for value in values:
		if value in temp_dict:
			temp_dict[value] = temp_dict[value] + 1
		else:
			temp_dict[value] = 1
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
			temp_list.append(word)
	return (city,temp_list)




