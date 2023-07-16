from pymongo import MongoClient

from .mongo_db_configs import mongo_db_infos


class DBConnectionHandler:
    def __init__(self) -> None:
        self.__connection_str = 'mongodb://{}:{}@{}:{}/?authSource=admin'.format( # noqa
            mongo_db_infos['username'],
            mongo_db_infos['password'],
            mongo_db_infos['host'],
            mongo_db_infos['port'],
        )
        self.__database_name = mongo_db_infos["database"]
        self.__client = None
        self.__connection = None

    def connection_to_db(self):
        self.__client = MongoClient(self.__connection_str)
        self.__connection = self.__client[self.__database_name]

    def get_connection(self):
        return self.__connection

    def get_client(self):
        return self.__client
