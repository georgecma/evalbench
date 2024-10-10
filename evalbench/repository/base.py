from abc import ABC, abstractmethod


class Repository(ABC):
    def __init__(self, repo_config):
        pass

    @abstractmethod
    def clone(self):
        raise NotImplementedError("Subclasses must implement this method")
