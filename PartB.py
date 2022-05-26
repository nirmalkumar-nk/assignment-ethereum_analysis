import pyspark, time

sc = pyspark.SparkContext()


def clean_trans(line):
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
        if fields[3] != 'value':
            to = str(fields[2])
            values = int(fields[3])
            return (to, values)
        else:
            return ('tmp', 0)
    except:
        return None


def clean_cons(line):
    try:
        fields = line.split(',')
        if len(fields) == 5 and fields[3] != 'block_number':
            return True
        else:
            return False
    except:
        return False


def get_contract_features(line):
    try:
        fields = line.split(',')
        if len(fields) == 5 and fields[3] != 'block_number':
            to = str(fields[0])
            return (str(to), 1)
        else:
            return (str('tmp_v'), 1)
    except:
        return (str('tmp_v'), 1)


print("App ID: {}".format(sc.applicationId))
start_time = time.time()
transactions = sc.textFile("/data/ethereum/transactions")
# lines = sc.textFile("hdfs://andromeda.eecs.qmul.ac.uk/user/np002/input")
contracts = sc.textFile("/data/ethereum/contracts")

###########
# cleansing
###########
clean_trans = transactions.filter(clean_trans)
print("transactions cleaned")

cleaned_cons = contracts.filter(clean_cons)
print("Contracts cleaned")

###########
# feature extraction
###########
trans_features = clean_trans.map(get_features)
print("transactions Features extracted")

con_features = cleaned_cons.map(get_contract_features)
print("con_features")

###########
# reducing or grouping
###########
trans_as_keys = trans_features.reduceByKey(lambda a, b: a + b)
print("transactions Keys Grouped")

con_reduced = con_features.reduceByKey(lambda a, b: a + b)

###########
# Performing Join
###########
joined = trans_as_keys.join(con_reduced)

###########
# Sorting
###########
top10 = joined.takeOrdered(10, key=lambda l: -l[1][0])

###########
# Printing the values
###########
for t in top10:
    print("address: {} value: {}".format(t[0], t[1][0]))
end_time = time.time()
print("App Execution time: {}".format(end_time - start_time))