from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


# Lista de campos com os caminhos e tipos
fields_list = [
    ("txt_obj_data.payload.id", "int"),
    ("txt_obj_data.payload.nome", "string"),
    ("txt_obj_data.payload.telefones", "array"),
    ("txt_obj_data.payload.residencia.rua", "string"),
    ("txt_obj_data.payload.residencia.bairro", "string"),
    ("txt_obj_data.payload.residencia.cep", "string"),
    ("txt_obj_data.error", "boolean"),
]

def get_spark_type(data_type):
    if data_type == "int":
        return T.IntegerType()
    elif data_type == "string":
        return T.StringType()
    elif data_type == "boolean":
        return T.BooleanType()
    elif data_type == "array":
        return T.ArrayType(T.StringType())  # Assumindo array de strings; ajuste conforme necessário
    else:
        raise ValueError(f"Tipo não suportado: {data_type}")

# Função para criar o esquema aninhado com base nos caminhos
def build_schema(fields):
    root = T.StructType()
    
    for path, data_type in fields:
        keys = path.split(".")
        current_schema = root
        
        # Percorrendo o caminho até o último campo
        for i, key in enumerate(keys):
            if i == len(keys) - 1:
                # Último campo, adiciona o tipo de dado
                current_schema.add(T.StructField(key, get_spark_type(data_type), True))
            else:
                # Se o campo ainda não existe, cria um campo aninhado
                existing_field = next((f for f in current_schema if f.name == key), None)
                
                if existing_field is None:
                    # Se o campo não existe, cria um StructType vazio
                    new_field = T.StructField(key, T.StructType(), True)
                    current_schema.add(new_field)
                    current_schema = new_field.dataType
                else:
                    # Se o campo já existe, continua aninhando
                    current_schema = existing_field.dataType
                    
    return root


# Criando o esquema
schema = build_schema(fields_list)

print(schema)
print("=====")
print(schema.json())