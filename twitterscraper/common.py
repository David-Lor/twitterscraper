import abc


class Service(abc.ABC):

    async def setup(self):
        pass

    async def teardown(self):
        pass


class Runnable(Service):

    @abc.abstractmethod
    async def run(self):
        pass
