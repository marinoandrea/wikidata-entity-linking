import dataclasses
import datetime
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from typing_extensions import TypedDict


class EntityLabel(Enum):
    """
    Enumeration of NER labels (same as spacy library).
    """
    PERSON = 'PERSON'
    NORP = 'NORP'
    FAC = 'FAC'
    ORG = 'ORG'
    GPE = 'GPE'
    LOC = 'LOC'
    PRODUCT = 'PRODUCT'
    EVENT = 'EVENT'
    WORK_OF_ART = 'WORK_OF_ART'
    LAW = 'LAW'
    LANGUAGE = 'LANGUAGE'
    DATE = 'DATE'
    TIME = 'TIME'


@dataclass(eq=True, frozen=True, unsafe_hash=False)
class NamedEntity:
    name: str
    label: EntityLabel


@dataclass(eq=True, frozen=True, unsafe_hash=False)
class CandidateNamedEntity:
    id: str
    score: float
    label: str
    description: str
    # TODO(andrea): actually extract ranges from text
    range_start: int = 0
    range_end: int = 0


@dataclass(eq=True, frozen=True, unsafe_hash=False)
class EntityMapping:
    named_entity: NamedEntity
    entity_url: Optional[str]


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
    # FIXME(andrea): we are still using trec_id instaed of record_id
    # because that's how the sample that we use for testing is formatted
    # However, it was mentioned on canvas that we should use record_id for
    # the submission. It is crucial that we swap these in the warc metadata
    # parsing and subsequent output to console.
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


@dataclass(eq=True, frozen=True, unsafe_hash=False)
class TridentQueryTask:
    warc_record: WARCRecordMetadata
    candidate: CandidateNamedEntity


@dataclass(eq=True, frozen=True, unsafe_hash=False)
class TridentQueryResult:
    _label: EntityLabel
    _candidate_id: str
    outdegree: int
    indegree: int
    is_label: bool
