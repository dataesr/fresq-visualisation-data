from project.server.main.format import get_formatted_data_filename
from project.server.main.logger import get_logger
from project.server.main.utils_swift import download_object

logger = get_logger(__name__)

import os
from typing import List, Optional

import jsonlines
from pymongo import InsertOne, MongoClient

MONGO_URI = os.getenv('MONGO_URI')
MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'fresq-visualisations')
ALIASES_COLLECTION = 'aliases'

client = None


def get_client():
    global client
    if client is None:
        if not MONGO_URI:
            raise ValueError('MONGO_URI environment variable is not set')
        client = MongoClient(MONGO_URI)
        logger.debug('MongoDB client connected')
    return client


def load_mongo(raw_data_suffix: str, index_name: Optional[str] = None) -> int:
    if index_name is None:
        index_name = f'fresq-{raw_data_suffix}'

    formatted_data_filename = get_formatted_data_filename(raw_data_suffix)
    download_object('fresq', formatted_data_filename, formatted_data_filename)

    # Read jsonl file
    with jsonlines.open(formatted_data_filename) as reader:
        data = [doc for doc in reader]

    if not data:
        logger.warning(f'No data found in {formatted_data_filename}')
        return 0

    db = get_client()[MONGO_DATABASE]
    if index_name in db.list_collection_names():
        logger.debug(f'Collection {index_name} already exists. Dropping...')
        db[index_name].drop()
        logger.debug(f'Collection {index_name} dropped')

    operations = [InsertOne(doc) for doc in data]
    result = db[index_name].bulk_write(operations, ordered=False)

    # Add index to aliases list
    db[ALIASES_COLLECTION].update_one(
        {'collection': 'programs'},
        {
            '$addToSet': {'indexes': index_name},
            '$setOnInsert': {'collection': 'programs'}
        },
        upsert=True
    )

    logger.debug(f'Import complete: {result.inserted_count} documents inserted into {index_name}')
    return result.inserted_count


def update_mongo_alias(index_name: str) -> bool:
    db = get_client()[MONGO_DATABASE]

    # Check if index exists in available indexes
    alias_doc = db[ALIASES_COLLECTION].find_one({'collection': 'programs'})
    available_indexes = alias_doc.get('indexes', []) if alias_doc else []

    if index_name not in available_indexes:
        logger.error(f'Index {index_name} not found in available indexes: {available_indexes}')
        return False

    db[ALIASES_COLLECTION].update_one(
        {'collection': 'programs'},
        {'$set': {'current': index_name}},
    )

    logger.info(f'Alias updated: programs now points to {index_name}')
    return True
