import pyspark
from datetime import date
import time

sc = pyspark.SparkContext()


def is_clean(line):
    try:
        fields = line.split(',')
        if len(fields) == 7:
            return True
        else:
            return False
    except:
        return False


def get_features(line):
    try:
        fields = line.split(',')
        dd_mm = date.fromtimestamp(int(fields[6]))
        dd_mm = str(dd_mm.year) + '_' + str(dd_mm.month)
        val = int(fields[3])
        r_t = (dd_mm, (val, 1))
        return r_t
    except:
        return (0, (1, 1))

start_time = time.time()
print("App ID: {}".format(sc.applicationId))
lines = sc.textFile("/data/ethereum/transactions")

###########
# cleansing
###########
clean_lines = lines.filter(is_clean)
# print("\nLines Cleaned")

###########
# feature extraction
###########
features = clean_lines.map(get_features)
# print("\nFeatures Extracted")

###########
# reducing or grouping
###########
grouped_keys = features.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
# print("\nKeys Grouped")

# g = grouped_keys.take(10)
# print("g\n")
# print(g)

values = grouped_keys.collect()
for v in values:
    print("{}, {}, {}".format(v[0], v[1][0], v[1][1]))


end_time = time.time()

print("App Execution time: {}".format(end_time - start_time)) 