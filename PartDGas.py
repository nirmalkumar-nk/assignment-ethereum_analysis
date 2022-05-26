import pyspark
from datetime import date

sc = pyspark.SparkContext()


def clean_transaction(line):
    try:
        fields = line.split(',')
        if len(fields) == 7 and fields[3] != 'value':
            return True
        else:
            return False
    except:
        return False


def get_features(line):
    try:
        fields = line.split(',')
        dd = date.fromtimestamp(int(fields[6]))
        dd_mm = str(dd.year) + '_' + str(dd.month)
        gas = int(fields[5])

        return (dd_mm, (gas, 1))

    except:
        return (0, (gas, 1))


def cal_avg(line):
    try:
        key = line[0]
        total_gas = int(line[1][0])
        total_counts = int(line[1][1])

        return (key, float(total_gas / total_counts))
    except:
        return (0, 0)


transaction = sc.textFile("/data/ethereum/transactions")
print("app_id,{0}".format(sc.applicationId))

###########
# cleansing
###########
clean_trans = transaction.filter(clean_transaction)
# print("Lines Cleaned")

###########
# feature extraction
###########
features = clean_trans.map(get_features)
# print("Features Extracted")

###########
# reducing or grouping
###########
grouped_keys = features.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
# print("Keys Grouped")

final_values = grouped_keys.map(cal_avg)
# print("Avg Done")

final_values = final_values.collect()

for fv in final_values:
    print("{},{}".format(fv[0], fv[1]))
