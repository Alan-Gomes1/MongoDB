import os

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

connection_str = f"mongodb://{os.getenv('USER')}:{os.getenv('PASSWORD')}\
    @localhost:27017/?authSource=admin"
client = MongoClient(connection_str)
db = client["mydb"]
collection = db.get_collection("collection")

response = collection.find({"ola": "mundo"})
[print(x) for x in response]

collection.insert_one({
    "Estou": "Inserindo",
    "Numeros": [123, 456]
})
