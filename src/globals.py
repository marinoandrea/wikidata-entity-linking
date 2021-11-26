import multiprocessing as mp
from typing import Dict

from src.interfaces import WARCJobInformation, WARCRecordMetadata
from src.utils import load_dumps

# multiprocessing setup
trident_queue: mp.Queue = mp.Queue()
manager = mp.Manager()
shared_dict: Dict[WARCRecordMetadata, WARCJobInformation] = manager.dict()

# dump dictionaries
dump_popular_entities = load_dumps(
    'person',  'city', 'country', 'org', 'software', 'website')
