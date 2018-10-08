import pickle
import os
import pymongo
from pymongo import DeleteMany
from pymongo.errors import BulkWriteError

pickle_list = []

input_dir = "/home/de-admin/Documents/open data/wikipedia final pickle/"

for p in os.listdir(input_dir):
    pickle_list.append(p)

db = pymongo.MongoClient().doc_store

for p in pickle_list[20:40]:
    with open(input_dir+p, "rb") as f:
        pickle_part = pickle.load(f)

    print("Pickle Loaded")
    
    try:
        db.raw.insert_many([{'_id': str(pp[0]), 'text': pp[1]} for pp in pickle_part], ordered=False).inserted_ids
        print("Data Inserted")
    except BulkWriteError as bwe:
        print(bwe.details)
