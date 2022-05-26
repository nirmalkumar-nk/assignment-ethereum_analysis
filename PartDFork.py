import pyspark

sc = pyspark.SparkContext()
forked_at = int('1476796771')  # Tangerine whistle
m_b = forked_at - int('2678400')  # month_before
m_a = forked_at + int('2678400')  # month_after
print("m_a: ", m_a)
print("m_b: ", m_b)


def clean_transactions(line):
    try:
        fields = line.split(',')
        if len(fields) == 7:
            time = int(fields[6])
            if m_b <= time <= m_a:  # only returns transactions that happened a month before and after the fork
                return True
            else:
                return False
        else:
            return False

    except:
        return False


def get_features(line):
    try:
        fields = line.split(',')
        time = int(fields[6])
        value = int(fields[3])
        gas_price = int(fields[4])
        if time >= m_b and time <= forked_at:
            return (0, (1, value, gas_price))
        elif time <= m_a and time >= forked_at:
            return (1, (1, value, gas_price))
    except:
        return (9999, (1, 1, 1))


def cal_avg(line):
    try:
        k = int(line[0])
        total_trans = int(line[1][0])
        total_value = int(line[1][1])
        total_gas = int(line[1][2])

        avg_trans = total_trans / 31
        avg_value = total_value / total_trans
        avg_gas = total_gas / total_trans
        return (k, (avg_trans, avg_value, avg_gas))
    except:
        return (9999, (1, 1, 1))


transactions = sc.textFile("/data/ethereum/transactions")
print("App ID: {0}".format(sc.applicationId))

###########
# cleansing
###########
clean_trans = transactions.filter(clean_transactions)
print("Transactions cleaned and filtered")
# ct = clean_trans.take(10)
# print("ct\n")
# print(ct)

###########
# feature extraction
###########
trans_features = clean_trans.map(get_features)
print("transactions Features extracted")
# tf = trans_features.take(10)
# print("tf\n")
# print(tf)

###########
# reducing or grouping
###########
# trans_as_keys = trans_features.reduceByKey(lambda a, b: ( a[1][0] + b[1][0], a[1][1] + b[1][1], a[1][2] + b[1][2]))
trans_as_keys = trans_features.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]))
print("transactions Keys Grouped")

final_values = trans_as_keys.map(cal_avg)
fv = final_values.collect()
print("Tangerine whistle")

for f in fv:
    print("time: {} , avg_trans: {} , avg_value: {} , avg_gas: {}".format(f[0], f[1][0], f[1][1], f[1][2]))