
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

def get_column_name_diff(table):
    return {
        "airlines" : _DIFF_AIRLINES(),
        "airports" : _DIFF_AIRPORTS(),
        "flights"  : _DIFF_FLIGHTS(),
        "planes"   : _DIFF_PLANES(),
        "weather"  : _DIFF_WEATHER()
    }.get(table)


def get_schema_struct(table):
    return {
        "airlines" : _SCHEMA_AIRLINES(),
        "airports" : _SCHEMA_AIRPORTS(),
        "flights"  : _SCHEMA_FLIGHTS(),
        "planes"   : _SCHEMA_PLANES(),
        "weather"  : _SCHEMA_WEATHER()
    }.get(table)

def get_primary_key(table):
    return {
        "airlines" : ["carrier"],
        "airports" : ["faa"],
        "flights"  : ["carrier", "flight", "year", "month", "day", "hour", "minute"],
        "planes"   : ["tailnum"],
        "weather"  : ["origin", "year", "month", "day", "hour"]
    }.get(table)


def _DIFF_AIRLINES():
    return {}


def _DIFF_AIRPORTS():
    return {
        "lat"   : "latitude",
        "lon"   : "longitude",
        "alt"   : "altitude",
        "tz"    : "timezone",
        "tzone" : "timezone_name" 
    }


def _DIFF_FLIGHTS():
    return {
        "dep_time": "actual_dep_time",
        "arr_time": "actual_arr_time"
    }


def _DIFF_PLANES():
    return {}


def _DIFF_WEATHER():
    return {}


def _SCHEMA_AIRLINES():
    return StructType([
        StructField("carrier", StringType(), False), 
        StructField("name",    StringType(), True)
    ])


def _SCHEMA_AIRPORTS():
    return StructType([
        StructField("faa",   StringType(),  False), 
        StructField("name",  StringType(),  True), 
        StructField("lat",   FloatType(),   True),
        StructField("lon",   FloatType(),   True),
        StructField("alt",   FloatType(),   True),
        StructField("tz",    IntegerType(), True),
        StructField("dst",   StringType(),  True),
        StructField("tzone", StringType(),  True)
    ])


def _SCHEMA_FLIGHTS():
    return StructType([
        StructField("index",          IntegerType(),   False),
        StructField("year",           IntegerType(),   True),
        StructField("month",          IntegerType(),   True),
        StructField("day",            IntegerType(),   True),
        StructField("dep_time",       IntegerType(),   True),
        StructField("sched_dep_time", IntegerType(),   False), 
        StructField("dep_delay",      IntegerType(),   True), 
        StructField("arr_time",       IntegerType(),   True),
        StructField("sched_arr_time", IntegerType(),   True),
        StructField("arr_delay",      IntegerType(),   True),
        StructField("carrier",        StringType(),    False), 
        StructField("flight",         IntegerType(),   True), 
        StructField("tailnum",        StringType(),    True),
        StructField("origin",         StringType(),    True),
        StructField("dest",           StringType(),    True),
        StructField("air_time",       FloatType(),     True),
        StructField("distance",       FloatType(),     True),
        StructField("hour",           IntegerType(),   True),
        StructField("minute",         IntegerType(),   True),
        StructField("time_hour",      TimestampType(), True)
    ])


def _SCHEMA_PLANES():
    return StructType([
        StructField("tailnum",      StringType(),  False), 
        StructField("year",         IntegerType(), True), 
        StructField("type",         StringType(),  True),
        StructField("manufacturer", StringType(),  True),
        StructField("model",        StringType(),  True),
        StructField("engines",      IntegerType(), True),
        StructField("seats",        IntegerType(), True),
        StructField("speed",        FloatType(),   True),
        StructField("engine",       StringType(),  True)
    ])


def _SCHEMA_WEATHER():
    return StructType([
        StructField("origin",     StringType(),    False), 
        StructField("year",       IntegerType(),   True), 
        StructField("month",      IntegerType(),   True),
        StructField("day",        IntegerType(),   True),
        StructField("hour",       IntegerType(),   True),
        StructField("temp",       FloatType(),   True),
        StructField("dewp",       FloatType(),   True),
        StructField("humid",      FloatType(),   True),
        StructField("wind_dir",   FloatType(),     False), 
        StructField("wind_speed", FloatType(),   True), 
        StructField("wind_gust",  FloatType(),   True),
        StructField("precip",     FloatType(),   True),
        StructField("pressure",   FloatType(),   True),
        StructField("visib",      FloatType(),   True),
        StructField("time_hour",  TimestampType(), True)
    ])
