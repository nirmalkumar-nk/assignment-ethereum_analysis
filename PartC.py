import pyspark, time

sc = pyspark.SparkContext()


def is_clean(line):
    try:
        fields = line.split(',')
        if len(fields) == 9 and fields[1] != 'hash':
            return True
        else:
            return False
    except:
        return False


def get_features(line):
    try:
        fields = line.split(',')
        miner = str(fields[2])
        size = int(fields[4])
        return (miner, size)
    except:
        return (0, 1)


blocks = sc.textFile("/data/ethereum/blocks")
print("App ID: {0}".format(sc.applicationId))
start_time =time.time()
###########
# cleansingim
###########
cleaned_blocks = blocks.filter(is_clean)
print("Blocks cleaned")

###########
# feature extraction
###########
block_features = cleaned_blocks.map(get_features)
print("Block features extracted")

###########
# reducing or grouping
###########
miner_values = block_features.reduceByKey(lambda a, b: a + b)
print("Miner Values aggregated")

###########
# Sorting
###########
top10 = miner_values.takeOrdered(10, key=lambda l: -l[1])

###########
# Printing the values
###########
for t in top10:
    print("miner: {} value: {}".format(t[0], t[1]))

end_time = time.time()
print("App Execution time: {}".format(end_time - start_time))

