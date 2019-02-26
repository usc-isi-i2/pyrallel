from abc import ABC, abstractmethod


class Paralleller(ABC):
    """
    Paralleller is an abstract class defines common methods for concrete Parallellers.
    """

    @abstractmethod
    def start(self):
        """
        Start processes and / or threads.
        """
        raise NotImplementedError

    @abstractmethod
    def add_task(self, *args, **kwargs):
        """
        Add new task.
        """
        raise NotImplementedError

    @abstractmethod
    def task_done(self):
        """
        All tasks are added.
        """
        raise NotImplementedError

    @abstractmethod
    def join(self):
        """
        Wait until all processes (threads) finish.
        """
        raise NotImplementedError

    def map(self, tasks: iter):
        """
        Syntactic sugar for adding task from an iterable object.

        Args:
            tasks (iter): Any iterable object.
        """
        for task in tasks:
            self.add_task(task)
