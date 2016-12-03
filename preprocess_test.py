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

data = event_df.rdd.map(lambda x: (x.city, Rake.run(x.data)[0:5])).collect()



temp_list = []

for event in data:
	city = event[0]
	for keyword in event[1]:
		words = (re.split(r'\s{1,}', keyword[0]))
		temp_list.append((city, words))

#print temp_list

dict_city = {}
dict_event = {}
for entry in temp_list:
	if entry[0] in dict_city:
		dict_city[entry[0]] = dict_city[entry[0]] + entry[1]
	else:
		dict_city[entry[0]] = entry[1]

for entry in temp_list:
	for event in entry[1]:
		if event in dict_event:
			dict_event[event].append(entry[0])
		else:
			dict_event[event] = [entry[0]]

dict_city_final = {}
for key,values in dict_city.iteritems():
	dict_Cevent_count = {}
	for val in values:
		if val in dict_Cevent_count:
			dict_Cevent_count[val] = dict_Cevent_count[val] + 1
		else:
			dict_Cevent_count[val] = 1
	dict_city_final[key] = dict_Cevent_count

print dict_city_final

print "-------------------------------------------------------"

dict_event_final = {}
for key,values in dict_event.iteritems():
	dict_Ecity_count = {}
	for val in values:
		if val in dict_Ecity_count:
			dict_Ecity_count[val] = dict_Ecity_count[val] + 1
		else:
			dict_Ecity_count[val] = 1
	dict_event_final[key] = dict_Ecity_count

print dict_event_final

# for keyword in keywords:
#     a = (re.split(r'\s{1,}', keyword[0])
#     for word in a:
#         temp_list.append(word)

