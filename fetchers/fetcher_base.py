from abc import ABC, abstractmethod
from typing import List

class FetcherBase(ABC):
    def __init__(self) -> None:
        super().__init__()
        self.lnk_hostname = 'siml4'  # SIML4
        self.saw_hostname = 'wiml4' # WIML4
        