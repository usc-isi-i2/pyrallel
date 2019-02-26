from abc import ABC, abstractmethod


class Paralleller(ABC):

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def add_task(self, *args, **kwargs):
        pass

    @abstractmethod
    def task_done(self):
        pass

    @abstractmethod
    def join(self):
        pass

    def map(self, tasks: iter):
        """
        Syntactic sugar for adding task from an iterable object.

        Args:
            tasks (iter): Any iterable object.
        """
        for task in tasks:
            self.add_task(task)
