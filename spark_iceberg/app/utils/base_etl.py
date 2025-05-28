from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class ToolETL(ABC):
    """Classe base abstrata para processos ETL."""
    
    def __init__(self, spark, file_path):
        self.spark = spark
        self.file_path = file_path

    def _extract(self) -> DataFrame:
        """Extrai dados do caminho especificado."""
        return (
            self.spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .option("delimiter", ",")
            .load(self.file_path)
        )

    @abstractmethod
    def _transform(self, df: DataFrame) -> DataFrame:
        """Método abstrato para transformações."""
        pass

    def run(self) -> DataFrame:
        """Executa o pipeline ETL completo."""
        df = self._extract()
        return self._transform(df)