from datetime import timedelta
from typing import Dict, List, Literal

from bson.objectid import ObjectId


class CollectionRepository:
    def __init__(self, db_connection):
        self.__collection_name = 'collection'
        self.__connection = db_connection

    def insert_document(self, document: Dict) -> Dict:
        """Inserts a document into the collection.

        Args:
            document (Dict): The document to be inserted.

        Returns:
            Dict: The inserted document."""

        collection = self.__connection.get_collection(self.__collection_name)
        collection.insert_one(document)
        return document

    def insert_list_of_documents(self, documents: List[Dict]) -> List[Dict]:
        """
        Insert a list of documents into the collection.

        Args:
            documents (List[Dict]): A list of dictionaries
            representing the documents to be inserted.

        Returns:
            List[Dict]: The list of inserted documents.
        """

        collection = self.__connection.get_collection(self.__collection_name)
        collection.insert_many(documents)
        return documents

    def select_many(self, filter: Dict) -> List[Dict]:
        """
        Retrieves a list of dictionaries from the collection
        based on the given filter.

        Args:
            filter (Dict): A dictionary representing the filter criteria.

        Returns:
            List[Dict]: A list of dictionaries containing the retrieved data.
        """

        collection = self.__connection.get_collection(self.__collection_name)
        data = collection.find(
            filter,
            {"_id": 0, "address": 0}
        )
        response = [element for element in data]
        return response

    def select_one(self, filter: Dict) -> Dict:
        """
        Retrieves a single document from the collection
        based on the given filter.

        Parameters:
            filter (Dict): The filter used to search for the document.

        Returns:
            Dict: The retrieved document, excluding the "_id"
            and "address" fields.
        """

        collection = self.__connection.get_collection(self.__collection_name)
        response = collection.find_one(filter, {"_id": 0, "address": 0})
        return response

    def select_if_property_exists(self, field: str) -> None:
        """
        Selects elements from the collection if the specified property exists.

        Args:
            field (str): The name of the property to check for existence.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries representing
            the selected elements.
        """

        collection = self.__connection.get_collection(self.__collection_name)
        data = collection.find({field: {"$exists": True}}, {"_id": 0})
        response = [element for element in data]
        return response

    def select_many_order(self, field: str, filter: Dict, order: Literal[1, -1]) -> List: # noqa
        """ Select elements from the collection in order

        Args: field (str): The name of the property to order the search.
        filter (dict): Name of the search filter elements.
        order (int): 1 sorts ascending, -1 sorts descending.

        Returns: List[Dict[str, Any]]: A list of dictionaries representing
        the selected elements. """

        collection = self.__connection.get_collection(self.__collection_name)
        data = collection.find(
            filter, {"_id": 0, "address": 0}
        ).sort([("requests." + field, order)])
        response = [element for element in data]
        return response

    def select_or(self, filter: Dict, exist: str) -> List[Dict]:
        """
        Selects elements from the collection based on the given filter
        and exist condition.

        Args:
            filter (Dict): A dictionary representing the filter condition.
            exist (str): The field to check for existence.

        Returns:
            List[Dict]: A list of dictionaries containing
            the selected elements.
        """

        collection = self.__connection.get_collection(self.__collection_name)
        data = collection.find(
            {"$or": [filter, {exist: {"$exists": True}}]},
            {"_id": 0}
        )
        response = [element for element in data]
        return response

    def select_by_object_id(self, arg: str) -> List[Dict]:
        """
        Selects documents from the collection by their object ID.

        Args:
            arg (str): The object ID to search for.

        Returns:
            List[Dict]: A list of dictionaries representing
            the selected documents.
        """

        collection = self.__connection.get_collection(self.__collection_name)
        data = collection.find({"_id": ObjectId(arg)})
        response = [element for element in data]
        return response

    def edit_registry(self, id: str, value: Dict) -> int:
        """Edit the registry with the given ID using the provided value.

        args:
            id (str) The ID of the registry to edit.
            value (Dict) A dictionary containing the new values
            to set in the registry.

        return: An integer representing the number of documents modified
        in the registry."""

        collection = self.__connection.get_collection(self.__collection_name)
        data = collection.update_one(
            {"_id": ObjectId(id)}, {"$set": value}
        )
        return data.modified_count

    def edit_many_registries(self, filter: Dict, value: Dict) -> int:
        """
        Edit many registries in the collection based on the given filter
        and update values.

        Args:
            filter (Dict): The filter to select the registries to update.
            value (Dict): The update values to apply
            to the selected registries.

        Returns:
            int: The number of registries that were successfully modified.
        """
        collection = self.__connection.get_collection(self.__collection_name)
        data = collection.update_many(filter, {"$set": value})
        return data.modified_count

    def edit_many_increment(self, filter: Dict, number: int) -> int:
        """
        Edit multiple documents in the collection by incrementing a specific
        field by a given number.

        Args:
            filter (Dict): A dictionary specifying the filter criteria
            for the documents to be updated.
            number (int): The number by which the specified field
            should be incremented.

        Returns:
            int: The number of documents modified in the collection.
        """
        collection = self.__connection.get_collection(self.__collection_name)
        data = collection.update_many(filter, {"$inc": number})
        return data.modified_count

    def delete_registry(self, key_value: Dict, id: str = None) -> int:
        """
        Delete registry from collection based on given key-value pair
        and optional id.

        Args:
            key_value (Dict): A dictionary representing the key-value pair
            to match with the document to delete.
            id (str, optional): The id of the document to delete.
            Defaults to None.

        Returns:
            int: The number of documents deleted.
        """
        collection = self.__connection.get_collection(self.__collection_name)
        data = collection.delete_one(
            {"$or": [{"_id": ObjectId(id)}, key_value]}
        )
        return data.deleted_count

    def create_index_ttl(self, key: str, ttl: timedelta) -> None:
        collection = self.__connection.get_collection(self.__collection_name)
        collection.create_index(key, expireAfterSeconds=ttl.seconds)
