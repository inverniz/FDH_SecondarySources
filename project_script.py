import requests, urllib, string, json
import pymongo
from pymongo import *

# returns a list of results
# documentation: https://dandelion.eu/docs/api/datatxt/nex/v1/#response
def dandelion_ner(text):
 #print "Dandelion"
 url = "https://api.dandelion.eu/datatxt/nex/v1"
 headers = {'text':text,'lang':'it','include':'types,lod,categories','epsilon':'0.3', 'token': 'ecd8d2b438484d92a593bf8274704cae'}
 r = requests.get(url,params=headers)
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
  
def main():
    client = MongoClient('128.178.60.49', 27017)
    client.linkedbooks_dev.authenticate('lb_pulse', '1243')
    db = client.linkedbooks_dev
    book_metadata = db.metadata.find({"type_document": "monograph"}, limit=1)
    for metadata in book_metadata:
        #print(metadata)
        bid = metadata["bid"]
        book = db.documents.find_one({"bid": bid})
        pages = book["pages"]
        fulltext = ""
        for page in pages:
            page = db.pages.find_one({"_id": page})
            text = page["fulltext"]
            fulltext = fulltext + text
        fulltext = fulltext.replace("\n", "")
        fulltext = fulltext.replace("\r", "")
        fulltext = fulltext.replace("\xa0", "")
        lines = fulltext.split(".")
        
        nb_lines = len(lines)
        text = ""
        i = 0
        j = 0
    
        # send max lines for request
        while j < nb_lines:
            while i < nb_lines and utf8len(text) < 3000:
                text = text + lines[i]
                i += 1
                print(i)
            results = dandelion_ner(text)
            print("Results:\n")
            print(results)
            j = i
            text = ""
    
        
if __name__ == "__main__":
    main()

