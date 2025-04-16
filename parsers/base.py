from abc import ABC, abstractmethod

class BaseParser(ABC):
    name: str  # должен быть переопределен в потомке

    @abstractmethod
    def parse(self, filepath: str):
        pass
