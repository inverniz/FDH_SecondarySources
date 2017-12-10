import requests, urllib, string, math, json, datetime, time
import pymongo
from pymongo import *

def get_time_to_wait():
	date = datetime.datetime.now()
	hours = date.hour * 3600
	minutes = date.minute * 60
	seconds = date.second

	total_sec = hours + minutes + seconds
	#print(hours, minutes, seconds, total_sec)
	total_time = 24 * 3600
	wait = total_time - total_sec + 10
	return wait
	
# returns a list of results
# documentation: https://dandelion.eu/docs/api/datatxt/nex/v1/#response
# this function is a modified version of the original one from Giovanni Colavizza.
def dandelion_ner(text, token):
 #print "Dandelion"
 url = "https://api.dandelion.eu/datatxt/nex/v1"
 headers = {'text':text,'lang':'it','include':'types,lod,categories','epsilon':'0.3', 'token': token}
 r = requests.post(url,data=headers)
 
 #print(r.headers)
 #print(r.text)
 
 	 
 if r.headers["content-type"] == "text/html":
     empty_result = list()
     return empty_result

 if r.status_code == requests.codes.ok:
	 data = json.loads(r.text)
	 if "error" in data.keys():
		 return "API error. Status: "+str(data['status'])+" Code: "+data['code']+" Message: "+data['message']
	 results = list()
	 
 # all entity types are dbpedia.org/ontology entities
	 for item in data['annotations']:
		 entity_type = ", ".join([w.split("/")[-1] for w in item['types']])
		 entity_label = item['label']
		 entity_spot = item['spot']
		 entity_category = ""
		 if "categories" in item.keys():
			 entity_category = ", ".join([w.split("/")[-1] for w in item['categories']])
			 dbpedia = ""
			 wikipedia = ""
		 if "lod" in item.keys():
			 dbpedia = item['lod']['dbpedia']
			 wikipedia = item['lod']['wikipedia']
			 results.append({"label": entity_label, "type": entity_type, "spot": entity_spot, "category": entity_category, "relevance": item['confidence'], "word": item['spot'], "offset": item['start'], "identifier": item['uri'], "title": item['title'], "dbpedia": dbpedia, "wikipedia": wikipedia})
 #print "id "+urllib.unquote(item['uri']).encode('latin-1')
	 return results
 else:
	 data = json.loads(r.text)
	 if "error" in data.keys():
		 print("API error. Status: "+str(data['status'])+" Code: "+data['code']+" Message: "+data['message'])
		 if (data["message"] == "no units left") and (data['status'] == 401):
			 wait = get_time_to_wait()
			 print("on attend pour " + str(wait) + " seconds")
			 time.sleep(wait)
		 empty_result = list()
		 return empty_result
		 
def utf8len(s):
	return len(s.encode('utf-8'))

def clean_text(text):
	text = text.replace("\n", "")
	text = text.replace("\r", "")
	text = text.replace("\xa0", " ")
	return text

def reformat_author(author):
	name = author.replace(" ", "").split(",")
	#to remove the date that are given with autor name
	reformated = name[1].split("<")[0]+ " " + name[0]
	return reformated.replace("-", "")

def entity_to_hashtag(entity):
	return "#" + entity.replace(" ", "").replace("-", "")

def author_to_hashtag(author):
	return "#" + reformat_author(author).replace(" ", "")

def authors_to_hashtag(authors):
	auth = ""
	for i in range(0, len(authors)):
		auth = auth + author_to_hashtag(authors[i]) + " "
	return auth

def title_to_hashtag(title):
	t = title.replace(":", " ")
	t_hashtag = t.replace(" ", "_")
	return "#" + t_hashtag

#connect to databases	
def connect():
	client = MongoClient('128.178.60.49', 27017)
	
	input_db = client.linkedbooks_dev
	input_db.authenticate('lb_pulse', '1243')
	
	output_db = client.lb_pulses
	output_db.authenticate('lb_pulse', '1243')
	
	return input_db, output_db

def scan_pages(input_db, entity, pages):
	entity_spot = entity["spot"]	
	
	for page_id in pages:
		page = input_db.pages.find_one({"_id": page_id})
		text = page["fulltext"]
		if entity_spot in text:
			page_number = int(page["printed_page_number"][0])
			return page_number+1
	
	return -1
		
def write_pulse_type1(entity, title, author, page_number, pages, output_db, input_db):
	entity_label = entity["label"]
	entity_spot = entity["spot"]
	wikipedia_resource = entity["wikipedia"]

	auth = reformat_author(author)
	
	if page_number == -1:
		page_number = scan_pages(input_db, entity, pages)
	
	pulse = entity_label + " (" + wikipedia_resource + ") " + "is present in book '" + title + "' by " + auth + " at page " + str(page_number) + ". " + entity_to_hashtag(entity_label) + " " + title_to_hashtag(title) + " " + author_to_hashtag(author)
	#print(pulse)
	#actual writing of the pulse
	pulse_id = output_db.pulses.insert({"type": 1, 
	"pulse": pulse, 
	"entity_name": entity_label, 
	"reference": entity_spot,
	"page_number": page_number, 
	"wikipedia_resource": wikipedia_resource})
	
	return pulse_id, page_number

def write_pulse_type1_articles(entity, title, author, journal_title, volume, page_number, pages, output_db, input_db):
	entity_label = entity["label"]
	entity_spot = entity["spot"]
	wikipedia_resource = entity["wikipedia"]
	
	if page_number == -1:
		page_number = scan_pages(input_db, entity, pages)

	authors = reformat_author(author[0])
	for i in range(1, len(author)):
		authors = authors + " and " + reformat_author(author[i])

	
	pulse = entity_label + " (" + wikipedia_resource + ") " + "is present in article '" + title + "' by " + authors + " at page " + str(page_number) + " in the volume " + volume + " of journal '" + journal_title + "'. " + entity_to_hashtag(entity_label) + " " + title_to_hashtag(title)  + " " + authors_to_hashtag(author) + title_to_hashtag(journal_title)
	#print(pulse)
	#actual writing of the pulse
	pulse_id = output_db.pulses.insert({"type": 1, 
	"pulse": pulse, 
	"entity_name": entity_label, 
	"reference": entity_spot,
	"page_number": page_number, 
	"wikipedia_resource": wikipedia_resource})
	
	return pulse_id, page_number
	
def write_pulse_type2(entity1, entity1_page_number, entity2, title, author, pages, output_db, input_db):
	entity1_label = entity1["label"]
	entity1_spot = entity1["spot"]
	entity1_wikipedia_resource = entity1["wikipedia"]
	
	entity2_label = entity2["label"]
	entity2_spot = entity2["spot"]
	entity2_wikipedia_resource = entity2["wikipedia"]
	
	entity2_page_number = scan_pages(input_db, entity2, pages)
	
	page_difference = math.ceil(math.fabs(entity1_page_number- entity2_page_number))

	auth = reformat_author(author)
	
	pulse = entity1_label + " (" + entity1_wikipedia_resource + ") " +  "and " + entity2_label + " (" + entity2_wikipedia_resource + ") " + "are " + str(page_difference) + " pages distant in the book '" + title + "' by " + auth + "." 
	#print(pulse)
	#actual writing of the pulse
	pulse_id = output_db.pulses.insert({"type": 2, 
	"pulse": pulse, 
	"entity1_name": entity1_label, 
	"entity1_page_number": entity1_page_number, 
	"entity1_wikipedia_resource": entity1_wikipedia_resource,
	"reference1": entity1_spot,
	"entity2_name": entity2_label, 
	"entity2_page_number": entity2_page_number, 
	"entity2_wikipedia_resource": entity2_wikipedia_resource,
	"reference2": entity2_spot,
	})
	
	return pulse_id, entity2_page_number

def write_pulse_type2_articles(entity1, entity1_page_number, entity2, title, author, journal_title, volume, pages, output_db, input_db):
	entity1_label = entity1["label"]
	entity1_spot = entity1["spot"]
	entity1_wikipedia_resource = entity1["wikipedia"]
	
	entity2_label = entity2["label"]
	entity2_spot = entity2["spot"]
	entity2_wikipedia_resource = entity2["wikipedia"]

	authors = reformat_author(author[0])
	for i in range(1, len(author)):
		authors = authors + " and " + reformat_author(author[i])
	
	entity2_page_number = scan_pages(input_db, entity2, pages)
	
	page_difference = math.ceil(math.fabs(entity1_page_number- entity2_page_number))
	
	pulse = entity1_label + " (" + entity1_wikipedia_resource + ") " +  "and " + entity2_label + " (" + entity2_wikipedia_resource + ") " + "are " + str(page_difference) + " pages distant in the article '" + title + "' by " + authors + " present in volume " + volume + " of journal '" + journal_title + "'." 
	#print(pulse)
	#actual writing of the pulse
	pulse_id = output_db.pulses.insert({"type": 2, 
	"pulse": pulse, 
	"entity1_name": entity1_label, 
	"entity1_page_number": entity1_page_number, 
	"entity1_wikipedia_resource": entity1_wikipedia_resource,
	"reference1": entity1_spot,
	"entity2_name": entity2_label, 
	"entity2_page_number": entity2_page_number, 
	"entity2_wikipedia_resource": entity2_wikipedia_resource,
	"reference2": entity2_spot,
	})
	
	return pulse_id, entity2_page_number

def write_pulses_copresence(entity1, entity1_page_number, entity2, title, pages, output_db, input_db):
	entity1_label = entity1["label"]
	entity1_spot = entity1["spot"]
	entity1_wikipedia_resource = entity1["wikipedia"]
	
	entity2_label = entity2["label"]
	entity2_spot = entity2["spot"]
	entity2_wikipedia_resource = entity2["wikipedia"]
	
	entity2_page_number = scan_pages(input_db, entity2, pages)
	
	page_difference = math.ceil(math.fabs(entity1_page_number- entity2_page_number))

	if page_difference == 0:
	
		pulse = "#copresence " + entity_to_hashtag(entity1_label) + " " + entity_to_hashtag(entity2_label) + " " + title_to_hashtag(title) + "_p" + str(entity1_page_number)
		#print(pulse)
		#actual writing of the pulse
		pulse_id = output_db.pulses.insert({"type": 2, 
		"pulse": pulse, 
		"entity1_name": entity1_label, 
		"entity1_page_number": entity1_page_number, 
		"entity1_wikipedia_resource": entity1_wikipedia_resource,
		"reference1": entity1_spot,
		"entity2_name": entity2_label, 
		"entity2_page_number": entity2_page_number, 
		"entity2_wikipedia_resource": entity2_wikipedia_resource,
		"reference2": entity2_spot,
		})
	
		return pulse_id, entity2_page_number
	else :
		return "", 0

def write_pulses_copresence_articles(entity1, entity1_page_number, entity2, title, pages, output_db, input_db):
	entity1_label = entity1["label"]
	entity1_spot = entity1["spot"]
	entity1_wikipedia_resource = entity1["wikipedia"]
	
	entity2_label = entity2["label"]
	entity2_spot = entity2["spot"]
	entity2_wikipedia_resource = entity2["wikipedia"]
	
	entity2_page_number = scan_pages(input_db, entity2, pages)

	pulse = "#copresence " + entity_to_hashtag(entity1_label) + " " + entity_to_hashtag(entity2_label) + " " + title_to_hashtag(title)
	#print(pulse)
	#actual writing of the pulse
	pulse_id = output_db.pulses.insert({"type": 2, 
	"pulse": pulse, 
	"entity1_name": entity1_label, 
	"entity1_page_number": entity1_page_number, 
	"entity1_wikipedia_resource": entity1_wikipedia_resource,
	"reference1": entity1_spot,
	"entity2_name": entity2_label, 
	"entity2_page_number": entity2_page_number, 
	"entity2_wikipedia_resource": entity2_wikipedia_resource,
	"reference2": entity2_spot,
	})
	
	return pulse_id, entity2_page_number

def write_pulses_mention_and_in(entity, title, page_number, pages, output_db, input_db):
	entity_label = entity["label"]
	entity_spot = entity["spot"]
	wikipedia_resource = entity["wikipedia"]
	
	if page_number == -1:
		page_number = scan_pages(input_db, entity, pages)
	
	pulse = "#mention " + entity_to_hashtag(entity_label) + " " + title_to_hashtag(title) + "_p" + str(page_number)
	pulse2 = "#in " + title_to_hashtag(title) + "_p" + str(page_number) + " " + title_to_hashtag(title)
	#print(pulse)
	#print(pulse2)
	pulse_id = output_db.pulses.insert({"type": 1, 
	"pulse": pulse, 
	"entity_name": entity_label, 
	"reference": entity_spot,
	"page_number": page_number, 
	"wikipedia_resource": wikipedia_resource})

	pulse_id2 = output_db.pulses.insert({"type": 1, 
	"pulse": pulse2, 
	"entity_name": entity_label, 
	"reference": entity_spot,
	"page_number": page_number, 
	"wikipedia_resource": wikipedia_resource})
	
	return pulse_id, page_number, pulse_id2

def write_pulses_mention_and_in_articles(entity, title, journal_title, volume, page_number, pages, output_db, input_db):
	entity_label = entity["label"]
	entity_spot = entity["spot"]
	wikipedia_resource = entity["wikipedia"]
	
	if page_number == -1:
		page_number = scan_pages(input_db, entity, pages)
	
	pulse = "#mention " + entity_to_hashtag(entity_label) + " " + title_to_hashtag(title) 
	pulse2 = "#in " + title_to_hashtag(title) + " " + title_to_hashtag(journal_title) + " vol." + str(volume)
	#print(pulse)
	#print(pulse2)
	#actual writing of the pulse
	pulse_id = output_db.pulses.insert({"type": 1, 
	"pulse": pulse, 
	"entity_name": entity_label, 
	"reference": entity_spot,
	"page_number": page_number, 
	"wikipedia_resource": wikipedia_resource})

	pulse_id2 = output_db.pulses.insert({"type": 1, 
	"pulse": pulse2, 
	"entity_name": entity_label, 
	"reference": entity_spot,
	"page_number": page_number, 
	"wikipedia_resource": wikipedia_resource})
	
	return pulse_id, page_number, pulse_id2

def write_pulses_eq(entity, title, page_number, pages, output_db, input_db):
	entity_label = entity["label"]
	entity_spot = entity["spot"]
	wikipedia_resource = entity["wikipedia"]
	
	if page_number == -1:
		page_number = scan_pages(input_db, entity, pages)
	
	pulse = "#eq " + entity_to_hashtag(entity_label) + " " + wikipedia_resource
	#print(pulse)
	#actual writing of the pulse
	pulse_id = output_db.pulses.insert({"type": 1, 
	"pulse": pulse, 
	"entity_name": entity_label, 
	"reference": entity_spot,
	"page_number": page_number, 
	"wikipedia_resource": wikipedia_resource})
	
	return pulse_id, page_number

def write_pulses_creator(entity, title, author, page_number, pages, output_db, input_db):
	entity_label = entity["label"]
	entity_spot = entity["spot"]
	wikipedia_resource = entity["wikipedia"]
	
	if page_number == -1:
		page_number = scan_pages(input_db, entity, pages)
	
	pulse = "#creator " + title_to_hashtag(title) + " " + author_to_hashtag(author)
	#print(pulse)
	#actual writing of the pulse
	pulse_id = output_db.pulses.insert({"type": 1, 
	"pulse": pulse, 
	"entity_name": entity_label, 
	"reference": entity_spot,
	"page_number": page_number, 
	"wikipedia_resource": wikipedia_resource})
	
	return pulse_id, page_number

def write_pulses_creator_articles(entity, title, authors, page_number, pages, output_db, input_db):
	entity_label = entity["label"]
	entity_spot = entity["spot"]
	wikipedia_resource = entity["wikipedia"]
	
	if page_number == -1:
		page_number = scan_pages(input_db, entity, pages)
	
	pulse = "#creator " + title_to_hashtag(title) + " " + authors_to_hashtag(authors)
	print(pulse)
	#actual writing of the pulse
	pulse_id = output_db.pulses.insert({"type": 1, 
	"pulse": pulse, 
	"entity_name": entity_label, 
	"reference": entity_spot,
	"page_number": page_number, 
	"wikipedia_resource": wikipedia_resource})
	
	return pulse_id, page_number

	
def write_pulses(results, metadata, pages, output_db, input_db, type):
	
	pulses_id = list()
	pulse_id1 = -1
	pulse_id2 = -1
	pulse_id3 = -1
	pulse_id4 = -1
	pulse_id5 = -1
	pulse_id6 = -1
	pulse_id7 = -1

	numb_entities = len(results)
	page_number_entity_1 = -1
	page_number_entity_2 = -1
	page_number_entity_3 = -1
	page_number_entity_4 = -1
	page_number_entity_5 = -1
	page_number_entity_6 = -1
	page_number_entity_7 = -1

	if type == "book":
		author = metadata["creator"]
		title = metadata["title"]["surface"]
	
		for index, entity_1 in enumerate(results):
			pulse_id1 = -1
			if page_number_entity_2 != -1:
				pulse_id1, page_number_entity_1 = write_pulse_type1(entity_1, title, author, page_number_entity_2, pages, output_db, input_db)
				pulse_id4, page_number_entity_4, pulse_id6 = write_pulses_mention_and_in(entity_1, title, page_number_entity_2, pages, output_db, input_db)
				pulse_id5, page_number_entity_5 = write_pulses_eq(entity_1, title, page_number_entity_2, pages, output_db, input_db)
				pulse_id7, page_number_entity_7 = write_pulses_creator(entity_1, title, author, page_number_entity_2, pages, output_db, input_db)
			else:
				pulse_id1, page_number_entity_1 = write_pulse_type1(entity_1, title, author, -1, pages, output_db, input_db)
				pulse_id4, page_number_entity_4, pulse_id6 = write_pulses_mention_and_in(entity_1, title, -1, pages, output_db, input_db)
				pulse_id5, page_number_entity_5 = write_pulses_eq(entity_1, title, -1, pages, output_db, input_db)
				pulse_id7, page_number_entity_7 = write_pulses_creator(entity_1, title, author, -1, pages, output_db, input_db)

			if index < numb_entities-1:
				entity_2 = results[index+1]
				pulse_id2, page_number_entity_2  = write_pulse_type2(entity_1, page_number_entity_1, entity_2, title, author, pages, output_db, input_db)
				pulse_id3, page_number_entity_3 = write_pulses_copresence(entity_1, page_number_entity_1, entity_2, title, pages, output_db, input_db)
			pulses_id.append(pulse_id1)
			pulses_id.append(pulse_id2)
			if(pulse_id3 != ""):
				pulses_id.append(pulse_id3)
			pulses_id.append(pulse_id4)
			pulses_id.append(pulse_id5)
			pulses_id.append(pulse_id6)
			pulses_id.append(pulse_id7)
	else:
		author = metadata["authors"]
		title = metadata["title"]
		journal_title = metadata["journal_short_title"]
		volume = metadata["volume"]

		for index, entity_1 in enumerate(results):
			pulse_id1 = -1
			if page_number_entity_2 != -1:
				pulse_id1, page_number_entity_1 = write_pulse_type1_articles(entity_1, title, author, journal_title, volume, page_number_entity_2, pages, output_db, input_db)
				pulse_id4, page_number_entity_4, pulse_id6 = write_pulses_mention_and_in_articles(entity_1, title, journal_title, volume, page_number_entity_2, pages, output_db, input_db)
				pulse_id5, page_number_entity_5 = write_pulses_eq(entity_1, title, page_number_entity_2, pages, output_db, input_db)
				pulse_id7, page_number_entity_7 = write_pulses_creator_articles(entity_1, title, author, page_number_entity_2, pages, output_db, input_db)

			else:
				pulse_id1, page_number_entity_1 = write_pulse_type1_articles(entity_1, title, author, journal_title, volume, -1, pages, output_db, input_db)
				pulse_id4, page_number_entity_4, pulse_id6 = write_pulses_mention_and_in_articles(entity_1, title, journal_title, volume, -1 , pages, output_db, input_db)
				pulse_id5, page_number_entity_5 = write_pulses_eq(entity_1, title, -1, pages, output_db, input_db)
				pulse_id7, page_number_entity_7 = write_pulses_creator_articles(entity_1, title, author, -1, pages, output_db, input_db)


			if index < numb_entities-1:
				entity_2 = results[index+1]
				pulse_id2, page_number_entity_2  = write_pulse_type2_articles(entity_1, page_number_entity_1, entity_2, title, author, journal_title, volume, pages, output_db, input_db)
				pulse_id3, page_number_entity_3 = write_pulses_copresence_articles(entity_1, page_number_entity_1, entity_2, title, pages, output_db, input_db)
			pulses_id.append(pulse_id1)
			pulses_id.append(pulse_id2)
			pulses_id.append(pulse_id3)
			pulses_id.append(pulse_id4)
			pulses_id.append(pulse_id5)
			pulses_id.append(pulse_id6)
			pulses_id.append(pulse_id7)

	
	return pulses_id
	
# write a books info on the output database
def write_book(results, metadata, pulses_id, output_db):
	output_db.books.insert_one({"creator": metadata["creator"], 
	"language": metadata["language"], 
	"img_bib2": metadata["img_bib"], 
	"type_catalogue": metadata["type_catalogue"], 
	"subjects": metadata["subjects"],
	"title": metadata["title"],
	"bid": metadata["bid"],
	"date": metadata["date"],
	"relations": metadata["relations"],
	"provenance": metadata["provenance"],
	"sbn_id": metadata["sbn_id"],
	"pulses": pulses_id
	 })
	return True

def write_articles(results, metadata, pulses_id, output_db):
	output_db.articles.insert_one({"authors": metadata["authors"], 
		"journal_bid": metadata["journal_bid"], 
		"journal_short_title": metadata["journal_short_title"], 
		"title": metadata["title"], 
		"year": metadata["year"], 
		"volume": metadata["volume"],
		"pulses": pulses_id})
	return True
	
def process_books(input_db, output_db, token_used):
	book_metadata = input_db.metadata.find({"type_document": "monograph"}, limit=5)
	
	for metadata in book_metadata:
		bid = metadata["bid"]
		book = input_db.documents.find_one({"bid": bid})
		#add is_ingested_ocr == true and dont_process == false condition
		pages = book["pages"]
		fulltext = ""
		
		for page in pages:
			page = input_db.pages.find_one({"_id": page})
			text = page["fulltext"]
			fulltext = fulltext + text
			
		fulltext = clean_text(fulltext)
		fulltext_length = len(fulltext)
		print("fulltext length:" + str(len(fulltext)))
		
		if fulltext_length < 950000:
			#print(fulltext_length)
			results = dandelion_ner(text, token_used)
			#print("Results: " + str(results))
			#keep processing
			pulses_id = write_pulses(results, metadata, pages, output_db, input_db, "book")
			write_book(results, metadata, pulses_id, output_db)
		else:
			lines = fulltext.split(".")
			nb_lines = len(lines)
			
			text = ""
			previous_text = ""
			i = 0
			j = 0
			
			while j < nb_lines:
				while i < nb_lines and len(text) < 950000:
					#print("length:" + str(utf8len(text)))
					previous_text = text
					text = text + ". " + lines[i]
					i += 1
				#print(text)
				if len(text) < 950000:
					j = i
				else:
					text = previous_text
					#to take the last line that we didn't take
					j = i - 1
				print("intermediary length: " + str(len(text)))
				results = dandelion_ner(text, token_used)
				#print("Results: " + str(results))
				#keep processing
				pulses_id = write_pulses(results, metadata, pages, output_db, input_db, "book")
				write_book(results, metadata, pulses_id, output_db)

				
				text = ""
				previous_text = ""

def process_articles(input_db, output_db, token_used):
	articles = input_db.bibliodb_articles.find({})
   
	for article in articles:
		#get metadata of articles
		metadata = {"authors": article["authors"], 
		"journal_bid": article["journal_bid"], 
		"journal_short_title": article["journal_short_title"], 
		"title": article["title"], 
		"year": article["year"], 
		"volume": article["volume"]}
		#get the id of the journal containing the article
		journal_id = article["document_id"]
		#get the place of the article in the journal
		number = article["internal_id"][-2:]
		if number[0] == ":":
			number = number[1]
		#-1 to have the right index as it starts from 0 for list
		number = int(number) - 1
		
		journal = input_db.documents.find_one({"_id": journal_id})
		ar = journal["articles"][number]
		start_page = int(ar["start_page"])
		end_page = int(ar["end_page"])

		fulltext = ""
		pages = []
	   
		for i in range(start_page, end_page + 1):
			pages.append(journal["pages"][i])
		
		for page in pages:
			page = input_db.pages.find_one({"_id": page})
			text = page["fulltext"]
			fulltext = fulltext + text

		fulltext = clean_text(fulltext)
		fulltext_length = len(fulltext)
		#print("fulltext length:" + str(len(fulltext)))

		if fulltext_length < 950000:
			results = dandelion_ner(text, token_used)
			#print("Results: " + str(results))
			pulses_id = write_pulses(results, metadata, pages, output_db, input_db, "journal")
			write_articles(results, metadata, pulses_id, output_db)
		else:
			lines = fulltext.split(".")
			nb_lines = len(lines)
			text = ""
			i = 0
			j = 0

			while j < nb_lines:
				while i < nb_lines and utf8len(text) < 950000:
					#print("length:" + str(utf8len(text)))
					text = text + lines[i]
					i += 1
				#print(text)
				results = dandelion_ner(text, token_used)
				#print("Results: " + str(results))
				pulses_id = write_pulses(results, metadata, pages, output_db, input_db, "journal")
				write_articles(results, metadata, pulses_id, output_db)
				j = i
				text = ""
   
def main():
	token_hakim = 'f3238f9b8e974df09b6814de9e9de532'
	token_marion = 'ecd8d2b438484d92a593bf8274704cae'
	token_used = token_marion
	
	input_db, output_db = connect()
	
	#process_books(input_db, output_db, token_used)
	process_articles(input_db, output_db, token_used)
	
		
if __name__ == "__main__":
	main()
