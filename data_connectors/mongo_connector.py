# TODO: method for mongo queries/filters, puke items into json 

import pymongo, sys, json
from pymongo import MongoClient
from faker import Faker



# connect to local database
myClient = MongoClient()


# returns list of dictionaries (ie: mongo docs) to be inserted into database
# create random names and/or dates(?)
def prepDocuments(numberOfRecods = 1):
    documents = []
    fake = Faker()

    
    for x in range(500):
        documents.append({"firstName":fake.first_name(), "lastName":fake.last_name()})


    print("randoms been generated")
    return documents




# takes in n docs and adds to mongodb collection
# take in db name, table name, num of docs, and if it should append to existing collection
def insertMongo(DB='pat', TABLE='test', numberofDocs=1, append=False):
    myDb = myClient[DB]
    myCol = myDb[TABLE]
    table = myDb.myCol

    documents = prepDocuments(numberofDocs)
    # documents = [{"firstName": "Cesar", "lastName": "Francis"},
                # {"firstName": "Connor", "lastName": "Pedroza"},
                # {"firstName": "Temma", "lastName": "Pedcris"}]


    # appending to table or else purging table and adding new data
    if append:
        table.insert(documents)
    else:
        table.drop()
        table.insert(documents)

    print ("Number of documents inserted: ", table.count())



# grabs all data from collection in mongodb database
def getMongo(DB = "pat", COLLECTION = "myCol"):
    db = myClient[DB]
    collection = db[COLLECTION]

    return collection.find({})



# insertMongo() inserts data into mongodb if none is already there or can append
# getMongo() returns the collection from a mongo database
def main():
    # insertMongo()

    returned = getMongo()




if __name__ == "__main__":
    main()