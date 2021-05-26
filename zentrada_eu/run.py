from zentrada_eu.main import DataWriter
from zentrada_eu.products import Products


class ScraperCore(DataWriter):
    def __init__(self):
        super(ScraperCore, self).__init__()
        self.scraper_run()
        self.main_output()

    @staticmethod
    def scraper_run():
        """
        Run all sub_classes
        """
        Products()

if __name__ == '__main__':
    ScraperCore()