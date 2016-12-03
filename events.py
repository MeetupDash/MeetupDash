from __future__ import unicode_literals

from HTMLParser import HTMLParser
import requests
import json
import time
import codecs
import sys
import csv
UTF8Writer = codecs.getwriter('utf8')
sys.stdout = UTF8Writer(sys.stdout)

def main():

	 cities = get_cities();
	 resultFile = open("output_final.csv",'wb')
	 wr = csv.writer(resultFile, dialect='excel', delimiter=b',')
	 api_key= "51422b7c6a44b375a5734645b44325c"

	 array = ["event_id", "event_name", "description","venue_id", "country", "city", "address", "location", "longitude", "lattitude", "group_id", "group_name", "group_desc", "group_lon", "group_lat", "group_urlname"]
	 wr.writerow(array)

	 country = ""
	 address = ""
	 location = ""
	 longitude = 0
	 lattitude = 0
	 venue_id = 0
	 event_id = ""
	 event_name = ""
	 description = ""
	 venue_id = ""
	 city = ""
	 location = ""

	 for (selected_city, selected_state) in cities:

	 	per_page = 200
	 	results_we_got = per_page
	 	offset = 0
	 	#time.sleep(5)
	 	while (results_we_got is not 0):
			response = get_events({"country":"US", "city":selected_city, "state":selected_state, "key":api_key, "page":per_page, "offset":offset})
			#print response
			time.sleep(5);
			offset = offset + 1

			# #print response
			if "problem" in response:
				print response;
				break;

			results_we_got = response['meta']['count']

			for event in response['results']:

				venue = ""

				if "id" in event:
					event_id = event['id'].encode('ascii', 'ignore').replace(',', ' ')

				if "name" in event:
					event_name = event['name'].encode('ascii', 'ignore').replace(',', ' ')

				if "group" in event:
					group_name = event['group']['name'].encode('ascii', 'ignore').replace(',', ' ')
					group_id = event['group']['id']
					if "who" in event['group']:
						group_desc = event['group']['who'].encode('ascii', 'ignore').replace(',', ' ')
					else:
						group_desc = ""
					group_lon = event['group']['group_lon']
					group_lat = event['group']['group_lat']
					group_urlname = event['group']['urlname'].encode('ascii', 'ignore').replace(',', ' ')

				if "venue" in event:
					country = event['venue']['localized_country_name'].encode('ascii', 'ignore').replace(',', ' ')
					#state = event['venue']['state']
					city = event['venue']['city'].encode('ascii', 'ignore').replace(',', ' ')
					address = event['venue']['address_1'].encode('ascii', 'ignore').replace(',', ' ')
					location = event['venue']['name'].encode('ascii', 'ignore').replace(',', ' ')
					longitude = event['venue']['lon']
					lattitude = event['venue']['lat']
					venue_id = event['venue']['id']


				if "description" in event:
					description_tags = event['description'].encode('ascii', 'ignore').replace(',', ' ').replace('\n', ' ').replace('\r', '')
					description = strip_tags(description_tags)

				array = [event_id, event_name, description,venue_id, country, city, address, location, longitude, lattitude, group_id, group_name, group_desc, group_lon, group_lat, group_urlname]
				wr.writerow(array)

			print results_we_got
#resultFile.close();

def get_events(params):

	print params;
	request = requests.get("http://api.meetup.com/2/open_events",params=params)
	data = request.json()
	return data

def get_cities():

	cities = []

	spamReader = csv.reader(open('Top5000Population.csv'), delimiter=b',')

	for row in spamReader:
		city = (row[0],row[1])
		cities.append(city)

	return cities

class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()

if __name__ == "__main__":
	main();