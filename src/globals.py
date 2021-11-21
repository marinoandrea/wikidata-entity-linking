import multiprocessing as mp
from typing import Dict

from src.interfaces import WARCJobInformation, WARCRecordMetadata

# multiprocessing setup
trident_queue: mp.Queue = mp.Queue()
manager = mp.Manager()
shared_dict: Dict[WARCRecordMetadata, WARCJobInformation] = manager.dict()
