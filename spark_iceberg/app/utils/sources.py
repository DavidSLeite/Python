from pyspark.sql import DataFrame, functions as F
from base_etl import ToolETL

class OrigemOLD(ToolETL):
    """
    Classe específica para tratar dados da Origem OLD.
    """
    
    def _transform(self, df: DataFrame) -> DataFrame:
        df_transform = (
            df
            .withColumn("tipo_cliente", F.lit('J'))
            .withColumn("data_venda", F.col('data_venda').cast('date'))
            .withColumn(
                "vlr_total",
                (F.col('vlr_preco').cast('double') * F.col('qtd_vendida').cast('double'))
                .cast('decimal(9,2)')
            )
        )
        return df_transform.filter(F.col('data_venda') <= '2025-01-25')


class OrigemNEW(ToolETL):
    """
    Classe específica para tratar dados da Origem NEW.
    """
    
    def _transform(self, df: DataFrame) -> DataFrame:
        df_transform = (
            df
            .withColumn("tipo_cliente", F.lit('J'))
            .withColumn("data_venda", F.col('data_venda').cast('date'))
            .withColumn(
                "vlr_total",
                (F.col('vlr_preco').cast('double') * F.col('qtd_vendida').cast('double'))
                .cast('decimal(9,2)')
            )
        )
        return df_transform.filter(F.col('data_venda') >= '2025-01-25')