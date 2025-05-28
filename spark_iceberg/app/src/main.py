
import sys
from awsglue.utils import getResolvedOptions
from utils.sources import OrigemOLD, OrigemNEW
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

args = getResolvedOptions(
    sys.argv, [
        'file_path_source_old',
        'file_path_source_new',
        'database_target',
        'table_target'
    ]
)

file_path_source_old = args['file_path_source_old']
file_path_source_new = args['file_path_source_new']
database_target = args['database_target']
table_target = args['table_target']

# Lendo os dados das origens OLD e NEW
source_old = OrigemOLD(spark, file_path_source_old)

source_new = OrigemNEW(spark, file_path_source_new)

# Transformando os dataframes com join
df_join = source_new.join(
    source_old,
    source_new["id_pedido"] == source_old["id_pedido"],
    how="inner"
)

# Convertendo o DataFrame para DynamicFrame
dyf = DynamicFrame.fromDF(df_join, glueContext, "dyf")

# Escrevendo o DynamicFrame no catálogo do Glue
glueContext.write_dynamic_frame.from_catalog(
    frame = dyf,
    database = database_target,
    table_name = table_target,
    additional_options = {
        "partitionKeys": ["ano", "mes"],
        "enableUpdateCatalog": True,
    }
)