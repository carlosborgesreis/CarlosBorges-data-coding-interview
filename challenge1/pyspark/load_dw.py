import os
import sys
import argparse
import logging
from pyspark.sql.functions import col, when
from pyspark.sql import SparkSession
from table_mappings import get_schema_struct, get_column_name_diff, get_primary_key

def _get_config(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', dest='host', type=str)
    parser.add_argument('--port', dest='port', type=int)
    parser.add_argument('--username', dest='username', type=str)
    parser.add_argument('--password', dest='password', type=str)
    parser.add_argument('--database', dest='database', type=str)
    parser.add_argument('--table', dest='table', type=str)
    parser.add_argument('--source', dest='source', type=str)
    parser.add_argument('--delimiter', dest='delimiter', type=str, default=',')
    config, _ = parser.parse_known_args(argv)

    return config


def _get_source_path(source):
    """Build absolute source file path

    :param source Source path
    """
    current_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_path, source)


def _write_to_db(data, config):
    return  data.write \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{config.host}:{config.port}/{config.database}") \
                .option("dbtable", config.table) \
                .option("user", config.username) \
                .option("password", config.password) \
                .option("driver", "org.postgresql.Driver") \
                .mode('append') \
                .save()


def _map_columns(data, schema):
    return data.select([col(c).alias(schema.get(c, c)) for c in data.columns])
 

def _read_csv(spark, source_path, schema, delimiter=','):
    return spark.read \
                .format("csv") \
                .option("header", True) \
                .option("delimiter", delimiter) \
                .schema(schema) \
                .load(source_path) \
                .na.replace('NA', None) 


def main(argv, spark):
    try:
        config = _get_config(argv)
        source_path = _get_source_path(config.source)

        raw_data = _read_csv(spark, source_path, get_schema_struct(config.table))
        mapped_data = _map_columns(raw_data, get_column_name_diff(config.table))

        if config.table == "flights":
            mapped_data = mapped_data.drop("index")
            #mapped_data_with_delay = mapped_data.withColumn("is_delayed", when((col("dep_delay") > 15), True).otherwise(False))

        return _write_to_db(mapped_data.dropDuplicates(get_primary_key(config.table)), config)


    except Exception as e:
        return logging.exception(e)


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .master("local") \
        .config("spark.jars", "C:\spark\spark-3.4.0-bin-hadoop3\jars\postgresql-42.6.0.jar") \
        .appName("load_dw") \
        .getOrCreate()

    logging.getLogger().setLevel(logging.INFO)
    main(sys.argv, spark)
