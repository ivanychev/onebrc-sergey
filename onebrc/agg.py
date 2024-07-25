def agg_zero():
    return [float("-inf"), float("inf"), 0, 0]


def reduce_agg(agg1, agg2):
    return [
        max(agg1[0], agg2[0]),
        min(agg1[1], agg2[1]),
        agg1[2] + agg2[2],
        agg1[3] + agg2[3],
    ]
