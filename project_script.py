import requests, urllib, string, math, json
import pymongo
from pymongo import *

# returns a list of results
# documentation: https://dandelion.eu/docs/api/datatxt/nex/v1/#response
# this function is a modified version of the original one from Giovanni Colavizza.
def dandelion_ner(text, token):
 #print "Dandelion"
 url = "https://api.dandelion.eu/datatxt/nex/v1"
 headers = {'text':text,'lang':'it','include':'types,lod,categories','epsilon':'0.3', 'token': token}
 r = requests.post(url,data=headers)
 if r.status_code == requests.codes.ok:
     data = json.loads(r.text)
     if "error" in data.keys():
         return "API error. Status: "+str(data['status'])+" Code: "+data['code']+" Message: "+data['message']
     results = list()
 # process Dandelion (Spazio dati) data
 # all entity types are dbpedia.org/ontology entities
     for item in data['annotations']:
         entity_type = ", ".join([w.split("/")[-1] for w in item['types']])
         entity_label = item['label']
         entity_category = ""
         if "categories" in item.keys():
             entity_category = ", ".join([w.split("/")[-1] for w in item['categories']])
             dbpedia = ""
             wikipedia = ""
         if "lod" in item.keys():
             dbpedia = item['lod']['dbpedia']
             wikipedia = item['lod']['wikipedia']
             results.append({"label": entity_label, "type": entity_type, "category": entity_category, "relevance": item['confidence'], "word": item['spot'], "offset": item['start'], "identifier": item['uri'], "title": item['title'], "dbpedia": dbpedia, "wikipedia": wikipedia})
 #print "id "+urllib.unquote(item['uri']).encode('latin-1')
     return results
 else:
  return "Web error: "+str(r.status_code)

def utf8len(s):
    return len(s.encode('utf-8'))

def clean_text(text):
	text = text.replace("\n", "")
	text = text.replace("\r", "")
	text = text.replace("\xa0", "")
	return text
	
def connect():
	client = MongoClient('128.178.60.49', 27017)
	
	input_db = client.linkedbooks_dev
	input_db.authenticate('lb_pulse', '1243')
	
	output_db = client.lb_pulses
	output_db.authenticate('lb_pulse', '1243')
	
	return input_db, output_db

def scan_pages(input_db, entity, pages):
	entity_label = entity["label"]	
	
	for page_id in pages:
		page = input_db.pages.find_one({"_id": page_id})
		text = page["fulltext"]
		if entity_label in text:
			page_number = page["printed_page_number"][0]
			return page_number
	
	return -1
		
def write_pulse_type1(entity, title, author, page_number, pages, output_db, input_db):
	entity_label = entity["label"]
	wikipedia_resource = entity["wikipedia"]
	
	if page_number == -1:
		page_number = scan_pages(input_db, entity, pages)
	
	pulse = entity_label + " (" + wikipedia_resource + ") " + "is present in book \"" + title + "\" by " + author + " at page " + str(page_number+1) + "."
	
	#actual writing of the pulse
	pulse_id = output_db.pulses.insert({"type": 1, 
	"pulse": pulse, 
	"entity_name": entity_label, 
	"page_number": page_number, 
	"wikipedia_resource": wikipedia_resource})
	
	return pulse_id, page_number
	
def write_pulse_type2(entity1, entity1_page_number, entity2, title, author, pages, output_db, input_db):
	entity1_label = entity1["label"]
	entity1_wikipedia_resource = entity1["wikipedia"]
	
	entity2_label = entity2["label"]
	entity2_wikipedia_resource = entity2["wikipedia"]
	
	entity2_page_number = scan_pages(input_db, entity2, pages)
	
	page_difference = math.fabs(entity1_page_number- entity2_page_number)
	
	pulse = entity1_label + " (" + entity1_wikipedia_resource + ") " +  " and " + entity2_label + " (" + entity2_wikipedia_resource + ") " + " are " + str(page_difference) + "pages distant in the book \"" + title + "\" by " + author + "." 
	
	#actual writing of the pulse
	pulse_id = output_db.pulses.insert({"type": 2, 
	"pulse": pulse, 
	"entity1_name": entity1_label, 
	"entity1_page_number": entity1_page_number, 
	"entity1_wikipedia_resource": entity1_wikipedia_resource,
	"entity1_name": entity2_label, 
	"entity1_page_number": entity2_page_number, 
	"entity1_wikipedia_resource": entity2_wikipedia_resource
	})
	
	return pulse_id, entity2_page_number
	
def write_pulses(results, metadata, pages, output_db, input_db):
	author = metadata["creator"]
	print("qui")
	title = metadata["title"]
	pulses_id = list()
	pulse_id1 = -1
	pulse_id2 = -1
	numb_entities = len(results)
	page_number_entity_1 = -1
	page_number_entity_2 = -1
	print("qui")
	for index, entity_1 in enumerate(results):
		pulse_id1 = -1
		if page_number_entity_2 != -1:
			pulse_id1, page_number_entity_1 = write_pulse_type1(entity_1, title, author, page_number_entity_1, pages, output_db, input_db)
		else:
			pulse_id1, page_number_entity_1 = write_pulse_type1(entity_1, title, author, -1, pages, output_db, input_db)
		if index < numb_entities-1:
			entity_2 = results[index+1]
			pulse_id2, page_number_entity_2  = write_pulse_type2(entity_1, page_number_entity_1, entity_2, title, author, pages, output_db, input_db)
	pulses_id.append(pulse_id1)
	pulses_id.append(pulse_id2)
	
	return pulses_id
	
# write a books info on the output database
def write_book(results, metadata, pulses_id, output_db):
	print(metadata["creator"])
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
	
def process_books(input_db, output_db, token_used):
	book_metadata = input_db.metadata.find({"type_document": "monograph"}, limit=1)
	
	for metadata in book_metadata:
		bid = metadata["bid"]
		book = input_db.documents.find_one({"bid": bid})
		#add is_ingested_ocr == true and dont_process == false condition
		pages = book["pages"]
		fulltext = ""
		print("here")
		for page in pages:
			page = input_db.pages.find_one({"_id": page})
			text = page["fulltext"]
			fulltext = fulltext + text
			
		fulltext = clean_text(fulltext)
		fulltext_length = len(fulltext)
		print("fulltext length:" + str(len(fulltext)))
		print("here")
		#if fulltext_length < 1000000:
		if False:
			results = dandelion_ner(text, token_used)
			print("Results: " + str(results))
			#keep processing
			pulses_id = write_pulses(results, metadata, pages, output_db, input_db)
			write_book(results, metadata, pulses_id, output_db)
		else:
			lines = fulltext.split(".")
			nb_lines = len(lines)
			
			text = ""
			i = 0
			j = 0
			print("here")
			#to remove after testing
			lines = lines[0] + "." + lines[1] + "." + lines[2]
			nb_lines = 3
			print("here")
			while j < nb_lines:
				while i < nb_lines and utf8len(text) < 1000000:
					print("length:" + str(utf8len(text)))
					text = lines
					i += 1
				print(text)
				results = dandelion_ner(text, token_used)
				print("Results: " + str(results))
				#keep processing
				pulses_id = write_pulses(results, metadata, pages, output_db, input_db)
				write_book(results, metadata, pulses_id, output_db)
				
				j = i
				text = ""
				
def main():
	token_hakim = 'f3238f9b8e974df09b6814de9e9de532'
	token_marion = 'ecd8d2b438484d92a593bf8274704cae'
	token_used = token_hakim
	
	print("here")
	input_db, output_db = connect()
	print("there")
	process_books(input_db, output_db, token_used)
	
    
        
if __name__ == "__main__":
    main()
