import requests, urllib, string, json
import pymongo
from pymongo import *

# returns a list of results
# documentation: https://dandelion.eu/docs/api/datatxt/nex/v1/#response
# this method is a modified version of the original one from Giovanni Colavizza-
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
         entity_category = ""
         if "categories" in item.keys():
             entity_category = ", ".join([w.split("/")[-1] for w in item['categories']])
             dbpedia = ""
             wikipedia = ""
         if "lod" in item.keys():
             dbpedia = item['lod']['dbpedia']
             wikipedia = item['lod']['wikipedia']
             results.append({"type": entity_type, "category": entity_category, "relevance": item['confidence'], "word": item['spot'], "offset": item['start'], "identifier": item['uri'], "title": item['title'], "dbpedia": dbpedia, "wikipedia": wikipedia})
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
	
def write_books(results, output_db):
	# write results in the books and pulses collections of the output_db
	return True
	
def process_books(input_db, output_db, token_used):
	book_metadata = input_db.metadata.find({"type_document": "monograph"}, limit=1)
	
	for metadata in book_metadata:
		bid = metadata["bid"]
		book = input_db.documents.find_one({"bid": bid})
		pages = book["pages"]
		fulltext = ""
		
		for page in pages:
			page = input_db.pages.find_one({"_id": page})
			text = page["fulltext"]
			fulltext = fulltext + text
			
		fulltext = clean_text(fulltext)
		fulltext_length = len(fulltext)
		print("fulltext length:" + str(len(fulltext)))
		
		if fulltext_length < 1000000:
			results = dandelion_ner(text, token_used)
			print("Results: " + str(results))
			#keep processing
			write_books(results, output_db)
		else:
			lines = fulltext.split(".")
			nb_lines = len(lines)
			
			text = ""
			i = 0
			j = 0
			
			while j < nb_lines:
				while i < nb_lines and utf8len(text) < 1000000:
					print("length:" + str(utf8len(text)))
					text = text + "." + lines[i]
					i += 1
				results = dandelion_ner(text, token_used)
				print("Results: " + str(results))
				#keep processing
				write_books(results, output_db)
				
				j = i
				text = ""
				
def main():
	token_hakim = 'f3238f9b8e974df09b6814de9e9de532'
	token_marion = 'ecd8d2b438484d92a593bf8274704cae'
	token_used = token_marion
	
	input_db, output_db = connect()
	
	process_books(input_db, output_db, token_used)
	
    
        
if __name__ == "__main__":
    main()
