import requests, math, json, datetime, time
from pymongo import MongoClient


def get_insertion_timestamp():
    date = datetime.datetime.now()
    return date
    
def get_time_to_wait():
    """return the number of seconds from now (when we call the function) until tomorrow
    """
    date = datetime.datetime.now()
    hours = date.hour * 3600
    minutes = date.minute * 60
    seconds = date.second

    total_sec = hours + minutes + seconds
    total_time = 24 * 3600
    wait = total_time - total_sec + 10
    return wait

def dandelion_ner(text, token):
    """returns a list of results
    documentation: https://dandelion.eu/docs/api/datatxt/nex/v1/#response
    this function is a modified version of the original one from Giovanni Colavizza.
    """
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
    """return length of a string in utf-8 econding.
    """
    return len(s.encode('utf-8'))

def clean_text(text):
    """clean text from whitespaces and newlines.
    """
    text = text.replace("\n", "")
    text = text.replace("\r", "")
    text = text.replace("\xa0", " ")
    return text

def reformat_author(author):
    """Reformat the name of the author, which is of type Surname, Name <birth, death> in Name Surname
    """
    name = author.replace(" ", "").split(",")
    #to remove the date that are given with author name
    if len(name) > 1:
        reformated = name[1].split("<")[0]+ " " + name[0]
    else:
        reformated = name[0]
    return reformated.replace("-", "")

def entity_to_hashtag(entity):
    """Transform the entity in a hashtag format
    """
    return "#" + entity.replace(" ", "").replace("-", "")

def author_to_hashtag(author):
    """Transform the name of the author in a hashtag format
    """
    if author == "":
        return "#unknown"
    else:
        return "#" + reformat_author(author).replace(" ", "")

def authors_to_hashtag(authors):
    """Transform a list of authors into a string consituted of all authors in a hashtag format 
    """
    auth = ""
    for i in range(0, len(authors)):
        auth = auth + author_to_hashtag(authors[i]) + " "
    return auth


def title_to_hashtag(title):
    """Transform the title in a hashtag format
    """
    t = title.replace(":", " ")
    t_hashtag = t.replace(" ", "_").replace(" ", "").replace("(", "").replace(")", "").replace(".", "_").replace("'", "_").replace("-", "_").replace('"', '_').replace(",", "")
    return "#" + t_hashtag.replace("___", "_")


def connect():
    """
    Connect to the two mongoDB databases.
    """
    client = MongoClient('128.178.60.49', 27017)
    
    input_db = client.linkedbooks_dev
    input_db.authenticate('lb_pulse', '1243')
    
    output_db = client.lb_pulses
    output_db.authenticate('lb_pulse', '1243')
    
    return input_db, output_db

def scan_pages(input_db, entity, pages):
    """Scan pages for a certain entity.
    input_db: the database we read from.
    entity: the entity we're looking for.
    pages: the id of the pages where to look.
    """
    entity_spot = entity["spot"]    
    
    for page_id in pages:
        page = input_db.pages.find_one({"_id": page_id})
        text = page["fulltext"]
        if entity_spot in text:
            page_number = int(page["printed_page_number"][0])
            return page_number+1
    
    return -1

def write_pulse_type1(entity, title, author, page_number, pages, output_db, input_db):
    """Create the first type of pulse for entities contained in books : "entity (link_wikipedia) is present in book 'book_title' by author_name at page page_number. #entity #book_title #author"
    entity: the first entity which is present in the book.
    page_number: the page number at which entity first appears.
    title: the title of the book.
    author: the author of the book.
    pages: the pages of the book.
    output_db: the database we write to.
    input_db: the database we read from.
    """  
    entity_label = entity["label"]
    wikipedia_resource = entity["wikipedia"]

    auth = reformat_author(author)
    
    if page_number == -1:
        page_number = scan_pages(input_db, entity, pages)
    
    pulse = entity_label + " (" + wikipedia_resource + ") " + "is present in book '" + title + "' by " + auth + " at page " + str(page_number) + ". " + entity_to_hashtag(entity_label) + " " + title_to_hashtag(title) + " " + author_to_hashtag(author)

    #actual writing of the pulse
    pulse_id = output_db.pulses.insert_one({"type": 1, 
    "pulse": pulse, 
    "entity_name": entity_label, 
    "page_number": page_number, 
    "wikipedia_resource": wikipedia_resource,
    "title": title,
    "author": author,
    "created_at": get_insertion_timestamp()
    }).inserted_id 
    
    return pulse_id, page_number

# Create the first type of pulse for entities contained in articles : "entity (link_wikipedia) is present in article 'article_title' by author(s)_name at page page_number in the volume volume_number of journal 'journal_title'. #entity #article_title #author #journal_title"
def write_pulse_type1_articles(entity, title, author, journal_title, volume, page_number, pages, output_db, input_db):
    """Create the first type of pulse for entities contained in articles : "entity (link_wikipedia) is present in article 'article_title' by author(s)_name at page page_number in the volume volume_number of journal 'journal_title'. #entity #article_title #author #journal_title"
    entity: the entity which is present in the article.
    title: the title of the article.
    author: the author of the article.
    journal_title: the title of the journal publishing the article.
    page_number: the page number where the entity was found.
    volume: the volume containing the journal.
    pages: the pages of the article.
    output_db: the database we write to.
    input_db: the database we read from.
    """  
    
    entity_label = entity["label"]
    wikipedia_resource = entity["wikipedia"]
    
    if page_number == -1:
        page_number = scan_pages(input_db, entity, pages)

    # As we can have multiple authors, we need to put them in string instead of a table 
    authors = reformat_author(author[0])
    for i in range(1, len(author)):
        authors = authors + " and " + reformat_author(author[i])

    
    pulse = entity_label + " (" + wikipedia_resource + ") " + "is present in article '" + title + "' by " + authors + " at page " + str(page_number) + " in the volume " + volume + " of journal '" + journal_title + "'. " + entity_to_hashtag(entity_label) + " " + title_to_hashtag(title)  + " " + authors_to_hashtag(author) + title_to_hashtag(journal_title)
    

    #actual writing of the pulse
    pulse_id = output_db.pulses.insert_one({"type": 1, 
    "pulse": pulse, 
    "entity_name": entity_label, 
    "page_number": page_number, 
    "wikipedia_resource": wikipedia_resource,
    "title": title,
    "author": author,
    "journal_title": journal_title,
    "volume": volume,
    "created_at": get_insertion_timestamp()}).inserted_id
    
    
    
    return pulse_id, page_number

def write_pulse_type2(entity1, entity1_page_number, entity2, title, author, pages, output_db, input_db):
    """Create a pulse when 2 entities are contained in the same book : "entity1 (link_wikipedia1) and entity2 (link_wikipedia2) are distance_in_page pages distant in the book 'book_title' by author_name. #entity1 #entity2 #book_title #author"
    entity1: the first entity which is present in the book.
    entity1_page_number: the page number at which entity1 first appears.
    entity2: the second entity that is present in the nook.
    title: the title of the book.
    author: the author of the book.
    pages: the pages of the book.
    output_db: the database we write to.
    input_db: the database we read from.
    """  
    entity1_label = entity1["label"]
    entity1_wikipedia_resource = entity1["wikipedia"]
    
    entity2_label = entity2["label"]
    entity2_wikipedia_resource = entity2["wikipedia"]
    
    entity2_page_number = scan_pages(input_db, entity2, pages)
    
    page_difference = math.ceil(math.fabs(entity1_page_number- entity2_page_number))

    auth = reformat_author(author)
    
    pulse = entity1_label + " (" + entity1_wikipedia_resource + ") " +  "and " + entity2_label + " (" + entity2_wikipedia_resource + ") " + "are " + str(page_difference) + " pages distant in the book '" + title + "' by " + auth + ". " + entity_to_hashtag(entity1_label) + " " + entity_to_hashtag(entity2_label) + " " + title_to_hashtag(title) + " " + author_to_hashtag(author)
    

    #actual writing of the pulse
    pulse_id = output_db.pulses.insert_one({"type": 2, 
    "pulse": pulse, 
    "entity1_name": entity1_label, 
    "entity1_page_number": entity1_page_number, 
    "entity1_wikipedia_resource": entity1_wikipedia_resource,
    "entity2_name": entity2_label, 
    "entity2_page_number": entity2_page_number, 
    "entity2_wikipedia_resource": entity2_wikipedia_resource,
    "title": title,
    "author": author,
    "created_at": get_insertion_timestamp()
    }).inserted_id
    
    return pulse_id, entity2_page_number

def write_pulse_type2_articles(entity1, entity1_page_number, entity2, title, author, journal_title, volume, pages, output_db, input_db):
    """Create a pulse when 2 entities are contained in the same article : "entity1 (link_wikipedia1) and entity2 (link_wikipedia2) are distance_in_page pages distant in the article 'article_title' by author(s)_name present in volume volume_number of journal 'journal_title'. #entity1 #entity2 #article_title #author(s) #journal_title"
    entity1: the first entity which is present in the article.
    entity1_page_number: the page number at which entity1 first appears.
    entity2: the second entity that is present in the article.
    title: the title of the article.
    author: the author of the article.
    journal_title: the title of the journal publishing the article.
    volume: the volume containing the journal.
    pages: the pages of the article.
    output_db: the database we write to.
    input_db: the database we read from.
    """  
    entity1_label = entity1["label"]
    entity1_wikipedia_resource = entity1["wikipedia"]
    
    entity2_label = entity2["label"]
    entity2_wikipedia_resource = entity2["wikipedia"]

    authors = reformat_author(author[0])
    for i in range(1, len(author)):
        authors = authors + " and " + reformat_author(author[i])
    
    entity2_page_number = scan_pages(input_db, entity2, pages)
    
    page_difference = math.ceil(math.fabs(entity1_page_number- entity2_page_number))
    
    pulse = entity1_label + " (" + entity1_wikipedia_resource + ") " +  "and " + entity2_label + " (" + entity2_wikipedia_resource + ") " + "are " + str(page_difference) + " pages distant in the article '" + title + "' by " + authors + " present in volume " + volume + " of journal '" + journal_title + "'. " + entity_to_hashtag(entity1_label) + " " + entity_to_hashtag(entity2_label) + " " + title_to_hashtag(title) + " " + authors_to_hashtag(author) + " " + title_to_hashtag(journal_title)
    

    
    #actual writing of the pulse
    pulse_id = output_db.pulses.insert_one({"type": 2, 
    "pulse": pulse, 
    "entity1_name": entity1_label, 
    "entity1_page_number": entity1_page_number, 
    "entity1_wikipedia_resource": entity1_wikipedia_resource,
    "entity2_name": entity2_label, 
    "entity2_page_number": entity2_page_number, 
    "entity2_wikipedia_resource": entity2_wikipedia_resource,
    "title": title,
    "author": author,
    "journal_title": journal_title,
    "volume": volume,
    "created_at": get_insertion_timestamp()
    }).inserted_id
    
    return pulse_id, entity2_page_number

def write_pulses_copresence(entity1, entity1_page_number, entity2, entity2_page_number, title, output_db):
    """Create a pulse of copresence for books, so we create a pulse when 2 entities are present on the same page of a book (here on page 42 for ex.) : #copresence #entity1 #entity2 #book_title_p42
    entity1: the first entity which is present in the article.
    entity2: the second entity that is present in the article.
    title: the title of the article.
    output_db: the database we write to.
    """  
    entity1_label = entity1["label"]
    entity2_label = entity2["label"]
    
    page_difference = math.ceil(math.fabs(entity1_page_number- entity2_page_number))

    if page_difference == 0:
    
        pulse = "#copresence " + entity_to_hashtag(entity1_label) + " " + entity_to_hashtag(entity2_label) + " " + title_to_hashtag(title) + "_p" + str(entity1_page_number)
        

        
        #actual writing of the pulse
        pulse_id = output_db.pulses.insert_one({"type": "book_copresence_pulse", 
        "pulse": pulse, 
        "entity1_name": entity1_label, 
        "entity1_page_number": entity1_page_number, 
        "entity2_name": entity2_label, 
        "entity2_page_number": entity2_page_number, 
        "title": title,
        "created_at": get_insertion_timestamp()
        }).inserted_id
        
        return pulse_id
    else :
        return ""

def write_pulses_copresence_articles(entity1, entity2, title, output_db):
    """Create a pulse of copresence for books, so we create a pulse when 2 entities are present in the same article : #copresence #entity1 #entity2 #article_title
    entity1: the first entity which is present in the article.
    entity2: the second entity that is present in the article.
    title: the title of the article.
    output_db: the database we write to.
    """  
    entity1_label = entity1["label"]
    entity2_label = entity2["label"]

    pulse = "#copresence " + entity_to_hashtag(entity1_label) + " " + entity_to_hashtag(entity2_label) + " " + title_to_hashtag(title)
    

    #actual writing of the pulse
    pulse_id = output_db.pulses.insert_one({"type": "article_copresence_pulse", 
    "pulse": pulse, 
    "entity1_name": entity1_label, 
    "entity2_name": entity2_label, 
    "title": title,
    "created_at": get_insertion_timestamp()
    }).inserted_id
    
    return pulse_id


def write_pulses_mention_and_in(entity, title, page_number, output_db): 
    """Create 2 different pulses.
    First the #mention pulse, which is a pulse that says in which book and page an entity is mentionned (here p.42 for ex.) : #mention #entity #book_title_p42
    Second the #in pulse, which is a pulse that say in which book we can find a certain page (here p.42 for ex.) : #in #book_title_p42 #book_title
    entity: the entity found in the article.
    title: the title of the book.
    page_number: the number of the page where the entity first appears in the book.
    output_db: the database we write to.
    """  
    entity_label = entity["label"]
    
    pulse = "#mention " + entity_to_hashtag(entity_label) + " #in " + title_to_hashtag(title) + "_p" + str(page_number)
    pulse2 = title_to_hashtag(title) + "_p" + str(page_number) + " #in " + title_to_hashtag(title)
    
    
    pulse_id = output_db.pulses.insert_one({"type": "book_mention_pulse", 
                                     "pulse": pulse, 
                                     "entity_name": entity_label, 
                                     "title": title,
                                     "page_number": page_number,
                                     "created_at": get_insertion_timestamp()}).inserted_id
    
    pulse_id2 = output_db.pulses.insert_one({"type": "book_in_pulse", 
                                         "pulse": pulse2, 
                                         "entity_name": entity_label, 
                                         "title": title,
                                         "page_number": page_number,
                                         "created_at": get_insertion_timestamp()}).inserted_id
    
    return pulse_id, pulse_id2


def write_pulses_mention_and_in_articles(entity, title, journal_title, volume, output_db):
    """Create 2 different pulses.
    First the #mention pulse, which is a pulse that says in which article an entity is mentionned : #mention #entity #article_title
    Second the #in pulse, which is a pulse that say in which journal we can find a certain article : #in #article_title #journal_title
    entity: the entity found in the article.
    title: the title of the article.
    journal_title: the title of the journal.
    volume: volume where to find the journal.
    output_db: the database we write to.
    """
    entity_label = entity["label"]
    pulse = "#mention " + entity_to_hashtag(entity_label) + " #in " + title_to_hashtag(title) 
    pulse2 = title_to_hashtag(title) + " #in " + title_to_hashtag(journal_title) + " vol." + str(volume)
    

    #actual writing of the pulse
    pulse_id = output_db.pulses.insert_one({"type": "article_mention_pulse", 
                                        "pulse": pulse, 
                                        "title": title,
                                        "created_at": get_insertion_timestamp()
                                        }).inserted_id
    
    pulse_id2 = output_db.pulses.insert_one({"type": "article_in_pulse", 
                                         "pulse": pulse2, 
                                         "title": title,
                                         "journal_title": journal_title,
                                         "volume": volume,
                                         "created_at": get_insertion_timestamp()
                                         }).inserted_id
    
    return pulse_id, pulse_id2

def write_pulses_eq(entity, output_db):
    """Create a pulse that associate an entity with a wikipedia URL : #eq #entity URL.
    entity: the entity to be associated to a wikipedia resource.
    output_db: the database we write to.
    """
    entity_label = entity["label"]
    wikipedia_resource = entity["wikipedia"]
    
    pulse = "#eq " + entity_to_hashtag(entity_label) + " " + wikipedia_resource
    

    #actual writing of the pulse
    pulse_id = output_db.pulses.insert_one({"type": "entity_eq_pulse", 
                                        "pulse": pulse, 
                                        "entity_name": entity_label, 
                                        "wikipedia_resource": wikipedia_resource,
                                        "created_at": get_insertion_timestamp()}).inserted_id
    
    return pulse_id

def write_pulses_creator(title, authors, output_db):
    """Create a pulse that links a book and its author : #creator #book_title #author.
    title: the title of the book.
    authors: the metadata of the book or book.
    output_db: the database we write to.
    """
    
    pulse = "#creator " + title_to_hashtag(title) + " " + author_to_hashtag(authors)
    #actual writing of the pulse
    pulse_id = output_db.pulses.insert_one({"type": "creator_pulse", 
                                        "pulse": pulse, 
                                        "title": title, 
                                        "authors": authors,
                                        "created_at": get_insertion_timestamp()}).inserted_id
    
    return pulse_id

def write_pulses_creator_articles(title, authors, output_db):
    """Create a pulse that links an article and its author(s) : #creator #article_title #author(s).
    title: the title of the article.
    authors: the metadata of the book or article.
    output_db: the database we write to.
    """
    
    pulse = "#creator " + title_to_hashtag(title) + " " + authors_to_hashtag(authors)
    
    # actual writing of the pulse
    pulse_id = output_db.pulses.insert_one({"type": "creator_pulse", 
    "pulse": pulse, 
    "title": title, 
    "authors": authors,
    "created_at": get_insertion_timestamp()}).inserted_id
    
    return pulse_id

    
def write_pulses(results, metadata, pages, output_db, input_db, type):
    """write the pulses associated to a book or article.
    results: the information on named entity recognition obtained from Dandelion.
    metadata: the metadata of the book or article.
    pages: the pages of the book or article.
    input_db: the database we read from.
    type: specifies if we're processing a book or an article.
    """
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
    
    if type == "book":
        author = metadata["creator"]
        title = metadata["title"]["surface"]
        for index, entity_1 in enumerate(results):
            pulse_id1 = -1
            if page_number_entity_2 != -1:
                pulse_id1, page_number_entity_1 = write_pulse_type1(entity_1, title, author, page_number_entity_2, pages, output_db, input_db)
            else:
                pulse_id1, page_number_entity_1 = write_pulse_type1(entity_1, title, author, -1, pages, output_db, input_db)
                
            pulse_id4, pulse_id6 = write_pulses_mention_and_in(entity_1, title, page_number_entity_1, output_db)
            pulse_id5 = write_pulses_eq(entity_1, output_db)
            pulse_id7 = write_pulses_creator(title, author, output_db)
            
            if index < numb_entities-1:        
                entity_2 = results[index+1]
                pulse_id2, page_number_entity_2  = write_pulse_type2(entity_1, page_number_entity_1, entity_2, title, author, pages, output_db, input_db)
                pulse_id3 = write_pulses_copresence(entity_1, page_number_entity_1, entity_2, page_number_entity_2, title, output_db)
                
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

            else:
                pulse_id1, page_number_entity_1 = write_pulse_type1_articles(entity_1, title, author, journal_title, volume, -1, pages, output_db, input_db)
            
            pulse_id4, pulse_id6 = write_pulses_mention_and_in_articles(entity_1, title, journal_title, volume, output_db)
            pulse_id5 = write_pulses_eq(entity_1, output_db)
            pulse_id7 = write_pulses_creator_articles(title, author, output_db)


            if index < numb_entities-1:
                entity_2 = results[index+1]
                pulse_id2, page_number_entity_2  = write_pulse_type2_articles(entity_1, page_number_entity_1, entity_2, title, author, journal_title, volume, pages, output_db, input_db)
                pulse_id3 = write_pulses_copresence_articles(entity_1, entity_2, title, output_db)
                
            pulses_id.append(pulse_id1)
            pulses_id.append(pulse_id2)
            pulses_id.append(pulse_id3)
            pulses_id.append(pulse_id4)
            pulses_id.append(pulse_id5)
            pulses_id.append(pulse_id6)
            pulses_id.append(pulse_id7)

    print("number of entites found: " + str(len(results)))
    print("number of pulse written: " + str(len(pulses_id)))
    
    return pulses_id

def write_book(results, metadata, pulses_id, output_db):
    """write a book in the database.
    metadata: the metadata of the book.
    pulses_id: the id of the pulses produced from this book.
    output_db: the database we write to.
    """
    
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
    "pulses": pulses_id,
    "created_at": get_insertion_timestamp()
     }).inserted_id
    
    return True
    

def write_articles(results, metadata, pulses_id, output_db):
    """write an article in the database.
    metadata: the metadata of the article.
    pulses_id: the id of the pulses produced from this article.
    output_db: the database we write to.
    """
    
    output_db.articles.insert_one({"authors": metadata["authors"], 
        "journal_bid": metadata["journal_bid"], 
        "journal_short_title": metadata["journal_short_title"], 
        "title": metadata["title"], 
        "year": metadata["year"], 
        "volume": metadata["volume"],
        "pulses": pulses_id,
        "created_at": get_insertion_timestamp()
        }).inserted_id
    
    return True


def process_books(input_db, output_db, token_used, testing):
    """processes all books in the database.
    input_db: the database we read from.
    output_db: the database we write to.
    token_used: the dandelion identifier to be used.
    """
    if testing:
        book_metadata = input_db.metadata.find({"type_document": "monograph"}, limit=1)
    else:
        book_metadata = input_db.metadata.find({"type_document": "monograph"})
    
    for index, metadata in enumerate(book_metadata):
        print("processing book number: " + str(index+1))
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
        #print("fulltext length:" + str(len(fulltext)))
        
        # if the book is short we send it in a single dandelion requests, otherwise we split the article in
        #several dandelion requests.
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
                #print("intermediary length: " + str(len(text)))
                results = dandelion_ner(text, token_used)
                #print("Results: " + str(results))
                #keep processing
                pulses_id = write_pulses(results, metadata, pages, output_db, input_db, "book")
                write_book(results, metadata, pulses_id, output_db)

                
                text = ""
                previous_text = ""

def process_articles(input_db, output_db, token_used, testing):
    """processes all articles in the database.
    input_db: the database we read from.
    output_db: the database we write to.
    token_used: the dandelion identifier to be used.
    """
    if testing:
        articles = input_db.bibliodb_articles.find({}, limit=1)
    else:
        articles = input_db.bibliodb_articles.find({})
   
    for index, article in enumerate(articles):
        print("processing article number: " + str(index+1))
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
            
        #-1 to have the right index as it starts from 0 for lists
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

        # if the article is short we send it in a single dandelion requests, otherwise we split the article in
        #several dandelion requests.
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
                    text = text + "." + lines[i]
                    i += 1
                #print(text)
                results = dandelion_ner(text, token_used)
                #print("Results: " + str(results))
                pulses_id = write_pulses(results, metadata, pages, output_db, input_db, "journal")
                write_articles(results, metadata, pulses_id, output_db)
                j = i
                text = ""
   
def main():
    #dandelion token to be used
    token_hakim = 'f3238f9b8e974df09b6814de9e9de532'
    token_marion = 'ecd8d2b438484d92a593bf8274704cae'
    token_used = token_marion
    
    # change to true if you want to test the script, false otherwise.
    testing = False
    
    print("trying to connect to databases")
    
    #connection to the two databases
    input_db, output_db = connect()
    
    print("connection established")
    
    print("start processing books")
    
    process_books(input_db, output_db, token_used, testing)
    
    print("all books processed")
    
    print("start processing articles")
    
    #process_articles(input_db, output_db, token_used, testing)
    
    print("done processing articles")
    
    print("script ended succesfully")
    
        
if __name__ == "__main__":
    main()