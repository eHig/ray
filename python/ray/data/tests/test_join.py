import random
from collections import defaultdict

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data._internal.push_based_shuffle import PushBasedShufflePlan
from ray.data.block import BlockAccessor
from ray.data.cogrouped import SimpleCoGroupPlan, JoinOp, SimpleJoin
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.util import extract_values
from ray.tests.conftest import *  # noqa


def test_partition_pandas(ray_start_regular):
    df = pd.DataFrame({"one": [1, 2, 3, 4, 5, 1, 1, 3], "two": ["a", "b", "c", "d", "e", "f", "g", "h"]})
    res = BlockAccessor.for_block(df).partition_by_key_hash(["one"], 2)
    for d in res:
        print(d.to_string())
    assert len(res) == 2
    assert sum([len(r) for r in res]) == len(df)
    assert 0 == 0

#todo partition tests
    # null handling
    # multiple keys
    # large random test
    # make it more property-based


def test_cogroup(ray_start_regular):
    l_df = pd.DataFrame({
        "one": [1, 2, 3, 4, 5, 1, 1, 3],
         "two": ["1-a", "2-b", "3-c", "4-d", "5-e", "1-f", "1-g", "3-h"]
    })

    r_df = pd.DataFrame({
        "one": [1, 2, 2, 3, 5],
        "three": ["1-a", "2-b", "2-c", "3-d", "5-e"]
    })

    l_ds = ray.data.from_pandas(l_df)
    l_ds = l_ds.repartition(2, shuffle=True).materialize()
    l_blocklist = l_ds._plan.execute()
    print(type(l_blocklist))


    r_ds = ray.data.from_pandas(r_df)
    r_ds = r_ds.repartition(2, shuffle=True).materialize()
    r_blocklist = r_ds._plan.execute()
    print(type(r_blocklist))

    left = (["one"], l_blocklist)
    right = (["one"], r_blocklist)

    join_plan = SimpleJoin()
    block_list, stats = join_plan.execute([left, right], 2, False)

    blocks = ray.get(block_list.get_blocks())
    output = pd.concat(blocks).reset_index(drop=True)

    print(f"output")
    print(output.to_string())
    #
    # try:
    #     res = join_plan.execute( [left, right], 2, False )
    # except Exception as e:
    #     print(e)

def test_init():
    p = 2
    s = 3
    nested_list = [[[] for _ in range(s)] for _ in range(p)]
    print(nested_list)

