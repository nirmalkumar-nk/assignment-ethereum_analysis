from mrjob.job import MRJob
from mrjob.step import MRStep


class PartBMR(MRJob):

    def mapper_join_tables(self, _, line):
        try:
            fields = line.split(",")

            if len(fields) == 5:
                add = fields[0]

                yield (add, (0, 0))

            elif len(fields) == 7:
                t_add = fields[2]

                t_value = int(fields[3])

                yield (t_add, (1, t_value))
        except:
            pass

    def combiner_yield(self, key, values):
        is_add_in_contracts = False

        total = 0

        for v in values:
            if v[0] == 0:
                is_add_in_contracts = True
            elif v[0] == 1:
                total += int(v[1])

        if is_add_in_contracts:
            yield (key, total)

    def reducer_join(self, key, values):
        is_add_in_contracts = False

        total = 0

        for v in values:
            if v[0] == 0:
                is_add_in_contracts = True
            elif v[0] == 1:
                total += int(v[1])

        if is_add_in_contracts:
            yield (key, total)

    def mapper_trans(self, key, value):
        try:
            if value > 0:
                yield ("value", (key, value))
        except:
            pass

    def dummy_reducer(self, key, value):
        try:
            if value > 0:
                yield ("value", (key, value))
        except:
            pass

    def reducer_final(self, key, values):
        sorted_values = sorted(values, reverse=True, key=lambda x: x[1])
        i = 0
        for v in sorted_values:
            while i < 10:
                i += 1
                yield (None, '{0},{1}'.format(v[0], int(v[1])))

    def steps(self):
        return [MRStep(mapper=self.mapper_join_tables, reducer=self.reducer_join,
                       jobconf={'mapreduce.job.reduces': '6'}),
                MRStep(mapper=self.mapper_trans, reducer=self.reducer_final, jobconf={'mapreduce.job.reduces': '36'})]


if __name__ == "__main__":
    PartBMR.run()
