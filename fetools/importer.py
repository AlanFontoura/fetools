# src/importer.py

from .local_importer import LocalImporter
from .s3_importer import S3Importer
from .api_importer import APIImporter

class DataImporter:
    def __init__(self):
        self.local_importer = LocalImporter()
        self.s3_importer = S3Importer()
        self.api_importer = APIImporter()

    def import_data(self, source, **kwargs):
        """
        Import data based on the source type.
        :param source: str, e.g., "local", "s3", "api"
        :param kwargs: Additional arguments for the specific importer
        :return: pandas DataFrame
        """
        if source == "local":
            return self.local_importer.import_data(**kwargs)
        elif source == "s3":
            return self.s3_importer.import_data(**kwargs)
        elif source == "api":
            return self.api_importer.import_data(**kwargs)
        else:
            raise ValueError(f"Unsupported source: {source}")