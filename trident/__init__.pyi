from typing import Iterator, List, Optional, Tuple, Union


class Db:

    def __init__(self, kb_path: str):
        ...

    @staticmethod
    def lookup_id(term: str) -> Optional[str]:
        ...

    @staticmethod
    def sparql(sql: str) -> str:
        ...

    @staticmethod
    def s(p: int, o: int) -> List[int]:
        ...

    @staticmethod
    def s_itr(p: int, o: int) -> Iterator:
        ...

    @staticmethod
    def s_aggr_fromo(o: int) -> List[int]:
        ...

    @staticmethod
    def o(s: int, p: int) -> List[int]:
        ...

    @staticmethod
    def o_aggr_froms(s: int) -> List[int]:
        ...

    @staticmethod
    def o_aggr_fromp(p: int) -> List[int]:
        ...

    @staticmethod
    def po(s: int) -> List[Tuple[int, int]]:
        ...

    @staticmethod
    def ps(o: int) -> List[Tuple[int, int]]:
        ...

    @staticmethod
    def os(p: int) -> List[Tuple[int, int]]:
        ...

    @staticmethod
    def n_s(p: int, o: int) -> int:
        ...

    @staticmethod
    def n_o(s: int, p: int) -> int:
        ...

    @staticmethod
    def count_s(s: int) -> int:
        ...

    @staticmethod
    def count_o(o: int) -> int:
        ...

    @staticmethod
    def count_p(p: int) -> int:
        ...

    @staticmethod
    def count_po(p: int, o: int) -> int:
        ...

    @staticmethod
    def exists(s: int, p: int, o: int) -> bool:
        ...

    @staticmethod
    def existsQuery(term: int, tuple: Tuple[int, int, int], pattern: str) -> bool:
        ...

    @staticmethod
    def all_s(text: int) -> List[Union[str, int]]:
        ...

    @staticmethod
    def all_p(text: int) -> List[Union[str, int]]:
        ...

    @staticmethod
    def all_o(text: int) -> List[Union[str, int]]:
        ...

    @staticmethod
    def lookup_relid(term: str) -> int:
        ...

    @staticmethod
    def lookup_str(id: int) -> str:
        ...

    @staticmethod
    def lookup_relstr(id: int) -> str:
        ...

    @staticmethod
    def search_id(term: str) -> List[Tuple[int, str]]:
        ...
