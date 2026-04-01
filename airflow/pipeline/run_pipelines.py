import luigi
from .tasks import *

class RootPipeline(luigi.WrapperTask):
    def required(self):
        pass

if __name__ == '__main__':
    luigi.run()
