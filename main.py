# Import some necessary modules
from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# Connect to MongoDB and pizza_data database
try:
   client = MongoClient('172.20.0.7',27017)
   db = client.vinomio_data
   print("Connected successfully!")
except:  
   print("Could not connect to MongoDB")
    
#connect kafka consumer to desired kafka topic	
consumer = KafkaConsumer('postgres.public.Producers',bootstrap_servers=['kafka:9092'])

for msg in consumer:
   print("message received?")
   try:
      if msg:
         record = json.loads(msg.value)
         operation = record and record['payload'] and record['payload']['op']
         if operation == 'c':
            data = record['payload']['after']
            try:
               document = {'name':data['name'], 'producerid':data['id']}
               print(f"....{document}")
               collection = db.producers.insert_one(document)
               print("Data inserted with record ids", collection)
            except  Exception as e:
                print("Could not insert into MongoDB")
                print(e)
         elif operation == 'u':
             data = record['payload']['after']
             try:
               filter = { 'producerid': data['id'] }
               newvalues = { "$set": { 'name': data['name']} }
               print(f"....{filter},{newvalues}")
               collection = db.producers.update_one(filter, newvalues, upsert=True) 
               print("Data updated with record ids", collection)
             except  Exception as e:
                print("Could not update into MongoDB")
                print(e)
         elif operation == 'd':
             data = record['payload']['before']
             try:
                filter = { 'producerid': data['id'] }
                print(f"....{filter}")
                collection = db.producers.delete_one(filter)
                print("Data delete with record ids", collection)
             except  Exception as e:
                print("Could not delete into MongoDB")
                print(e)
         else:
            print(f"Operation {operation} has not been implemented")
      else:
         print("msg is null")
   except Exception as e:
       print("Error parsing record")
       print(e)

    