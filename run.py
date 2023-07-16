from datetime import datetime, timedelta

from bson.objectid import ObjectId

from models.connection_options.connection import DBConnectionHandler
from models.repository.collection import CollectionRepository

db_handler = DBConnectionHandler()
db_handler.connection_to_db()
db_connection = db_handler.get_connection()

collection_repo = CollectionRepository(db_connection)


if __name__ == "__main__":

    order = {
        "name": "test",
        "address": "any",
        "requests": {
            "pizza": 2,
            "hamburguer": 5,
            "refri": 3
        }
    }
    collection_repo.insert_document(order)

    print(collection_repo.select_many({"name": "test"}), "\n")
    print(collection_repo.select_one({"name": "test"}))
    print(collection_repo.select_if_property_exists("_id"))
    print(collection_repo.select_many_order("pizza", {"name": "test"}, -1))
    print(collection_repo.select_or({"name": "test"}, "ola"))
    print(collection_repo.select_by_object_id('64ac6f32b67c7e456e72a062'))

    print(collection_repo.edit_registry(
        "64a984cb402c41a8301aeb6b", {"Estou": "aqui"}
    ))
    print(collection_repo.edit_many_registries(
        {"Numeros": 1}, {"Numeros.0": 0}
    ))
    print(collection_repo.edit_many_increment(
        {"_id": ObjectId("64a984cb402c41a8301aeb6b")}, {"Numeros.1": 4}
    ))
    print(collection_repo.delete_registry({"Estou": "aqui"}))
    print(collection_repo.create_index_ttl(
        "creation_date", timedelta(seconds=15)
    ))
    document = {"name": "Alan", "age": 26, "creation_date": datetime.now()}
    collection_repo.insert_document(document)
