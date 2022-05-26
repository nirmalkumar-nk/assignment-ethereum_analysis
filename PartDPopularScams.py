from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructField, StructType, LongType

json_path = "/data/ethereum/scams.json"
csv_path = "/data/ethereum/transactions"

spark = SparkSession.builder.appName("PartDScams By NK").getOrCreate()

scams_data = []


def load_json():
    print("App ID: {}".format(spark.sparkContext.applicationId))
    return spark.read.option("multiline", "true").json(json_path)

def load_transaction():
    trans_schema = StructType([StructField("block_number", StringType(), True),
                               StructField("from_address", StringType(), True),
                               StructField("to_address", StringType(), True),
                               StructField("value", LongType(), True),
                               StructField("gas", StringType(), True),
                               StructField("gas_price", StringType(), True),
                               StructField("block_timestamp", StringType(), True)])
    return spark.read.option("multiline", "true").schema(trans_schema).csv(csv_path)


def prep_json(scamsDF):
    try:
        count = scamsDF.count()
        rows = scamsDF.head(count)
        scams_list = []

        for r in rows:
            scam_dict = r['result'].asDict()
            keys = scam_dict.keys()
            for k in keys:
                scam = scam_dict[k].asDict()
                cat = str(scam['category'])
                stat = str(scam['status'])
                adds = scam['addresses']
                for a in adds:
                    t = (str(a), cat, stat)
                    scams_list.append(t)

        scams_schema = StructType([StructField("address_id", StringType(), True),
                                   StructField("category", StringType(), True),
                                   StructField("status", StringType(), True)])

        return spark.createDataFrame(data=scams_list, schema=scams_schema)
    except:
        return None


jsonFile = load_json()
csvDF = load_transaction()
jsonDF = prep_json(jsonFile)

joinedDF = jsonDF.join(csvDF, jsonDF.address_id == csvDF.from_address, 'inner')
joinedDF = joinedDF.withColumn("value", col("value").cast(LongType()))
table = joinedDF.sort("value", ascending=False).take(100)
# table = table.collect()
for row in table:
    print("{},{},{},{},{},{}".format(row[0],row[1],row[2],row[4],row[5],row[6]))

# joinedDF.show()