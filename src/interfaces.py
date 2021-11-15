import datetime
from dataclasses import dataclass
from typing import List, Optional

from typing_extensions import TypedDict


@dataclass(eq=True, frozen=True, unsafe_hash=False)
class NamedEntity:
    name: str
    label: str


@dataclass(eq=True, frozen=True, unsafe_hash=False)
class EntityMapping:
    named_entity: NamedEntity
    entity_url: str


@dataclass(eq=True, frozen=True, unsafe_hash=False)
class WARCRecordMetadata:
    """
    WARC record specific information is stored in this struct.
    Since we use the Trec ID as primary key for most operations
    this is the only required field.
    Example of warc record metadata: 

    ```
    WARC-Type: response, 
    WARC-Date: 2012-02-10T21:49:38Z, 
    WARC-TREC-ID: clueweb12-0000tw-00-00010, 
    WARC-IP-Address: 100.42.59.11
    WARC-Payload-Digest: sha1:3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ, 
    WARC-Target-URI: http://claywginn.com/wp-content/plugins/wordpress-file-monitor/wordpress-file-monitor.php, 
    WARC-Record-ID: <urn:uuid:f638c914-658d-418a-93f8-6e89a4858359>
    ```
    """
    trec_id: str
    w_type: Optional[str] = None
    date: Optional[datetime.datetime] = None
    ip_addr: Optional[str] = None
    digest: Optional[str] = None
    uri: Optional[str] = None
    record_id: Optional[str] = None


class WARCJobInformation(TypedDict):
    """
    Utility class used by the multiprocessing Manager dict.
    """
    mappings: List[EntityMapping]
    is_done: bool
    is_flushed: bool
