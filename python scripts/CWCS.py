import pymongo
from pymongo.errors import BulkWriteError

#Check mongodb is running in the terminal
db = pymongo.MongoClient().doc_store

# In order to reset database
# db.count.drop()

#Make a generator to call documents one at a time from mongo

def make_corpus():
    full_corpus = db.raw.find({})
    i = 0
    
    if db.count.find_one({}) == None:
        get_records = 1
    else:
        get_records = 0
        latest = db.count.find({}, {"_id":1}).limit(1).sort('$natural', pymongo.DESCENDING)[0]["_id"]
    
    for record in full_corpus:
        if i % 10000 == 0:
            print(i)
        
        i = i + 1
        
        if get_records == 1:
            yield record
            
        elif record["_id"] == latest:
            get_records = 1
            db.count.delete_one({'_id': latest})            
            yield record
        
        else:
            continue

from nltk import FreqDist

corpus = make_corpus()
error_log = []

for s in corpus:
    word_dist = FreqDist()
    word_dist.update(s["text"].split())
    
    try:
        db.count.insert_one({'_id': s["_id"], 'count': word_dist})
    except Exception as e:
        error_log.append(e)
        continue
