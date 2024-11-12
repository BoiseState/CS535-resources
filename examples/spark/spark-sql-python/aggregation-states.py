from collections import defaultdict

class SumAggregation:
    def __init__(self):
        self.total = 0

    def reduce(self, value):
        self.total += value

    def merge(a, b):
        a.total += b.total
        return a

    def finish(self):
        return self.total

class AverageAggregation:
    def __init__(self):
        self.total = 0
        self.count = 0

    def reduce(self, value):
        self.total += value
        self.count += 1

    def merge(a, b):
        a.total += b.total
        a.count += b.count
        return a

    def finish(self):
        return self.total / self.count

class CountDistinctAggregation:
    def __init__(self):
        self.uniq = set()

    def reduce(self, value):
        self.uniq.add(value)

    def merge(a, b):
        if len(a.uniq) < len(b.uniq):
            return CountDistinctAggregation.merge(b, a)
        for val in b.uniq:
            a.uniq.add(val)
        return a

    def finish(self):
        return len(self.uniq)

def hash_agg_partitions(
    agg_state_class, 
    ps, 
    agg_key,
    agg_col,
):
    shuffle_partitions = []
    for _ in range(len(ps)):
        shuffle_partitions.append([])
    for p in ps:
        aggs = defaultdict(lambda: agg_state_class())
        for row in p:
            key = agg_key(row)
            state = aggs[key]
            state.reduce(agg_col(row))
        print([(k, agg.finish()) for (k, agg) in aggs.items()])
        for (key, agg_state) in aggs.items():
            shuffle_partitions[key.__hash__() % len(shuffle_partitions)].append((key, agg_state))
    print("shuffling")
    for sp in shuffle_partitions:
        print([(k, agg.finish()) for (k, agg) in sp])
    post_agg_partitions = []
    for sp in shuffle_partitions:
        aggs = defaultdict(lambda: agg_state_class())
        for (key, agg_state) in sp:
            aggs[key] = agg_state_class.merge(aggs[key], agg_state)
        post_agg_partitions.append([(k, agg.finish()) for (k, agg) in aggs.items()])
    print("aggregating")
    for pa in post_agg_partitions:
        print(pa)

partitions = [
    [2, 3, 5, 1, 6, 7],
    [2, 7, 3, 90, 10],
    [60, 8, 40, 23, 4, 1, 8, 5, 13],
]

key_ps = [
    [(x % 2 == 0, x) for x in p]
    for p in partitions
]

hash_agg_partitions(
    SumAggregation, 
    key_ps, 
    lambda x: x[0],
    lambda x: x[1],
)