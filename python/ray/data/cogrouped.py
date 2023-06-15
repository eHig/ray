import pandas as pd

from ray.data._internal import stats
from ray.data._internal.pandas_block import PandasBlockAccessor

from ray.data._internal.block_list import BlockList
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data.block import Block, BlockAccessor, BlockExecStats, BlockMetadata
from typing import Any, Dict, List, Optional, Tuple


class CoGroupOp:

    def __init__(self, map_args: List[Any] = None, reduce_args: List[Any] = None):
        self._map_args = map_args or []
        self._reduce_args = reduce_args or []
        assert isinstance(self._map_args, list)
        assert isinstance(self._reduce_args, list)
    @staticmethod
    def reduce(source_blocks: List[List[Block]]) -> Tuple[Block, BlockMetadata]:
        print("reducing")

class JoinOp:
    @staticmethod
    def reduce(source_keys: List[str],
               source_blocks: List[List[Block]]
               ) -> Tuple[Block, BlockMetadata]:
        assert len(source_blocks) == len(source_keys)
        print(f"reducing join {len(source_blocks)}")
        # print(source_blocks)
        source_data = []
        for i, s in enumerate(source_blocks):
            print(f"source {i}: - type {type(s)} - len {len(s)}")
            df = s[0]
            for block in s[1:]:
                df = pd.concat([df, block]).reset_index(drop=True)
            source_data.append(df)
            # print("----------")
        l = source_data[0]
        l_keys = source_keys[0]
        for source_ind in range(1,len(source_data)):
            r = source_data[source_ind]
            print(f"--- joining merged source partitions: \n{l.to_string()} \n{r.to_string()}")
            l = pd.merge(l, r, left_on=l_keys, right_on=source_keys[source_ind])

        print("result of join reduce:")
        print(l.to_string())
        return l, PandasBlockAccessor(l).get_metadata(None, exec_stats=BlockExecStats.builder().build())

class SimpleCoGroupPlan(CoGroupOp):

    @staticmethod
    def map(
        idx: int,
        block: Block,
        keys: List[str],
        n_partitions: int,
    ) -> Tuple[BlockMetadata, List[Block]]:
        print("co-grouping")
        partitions = BlockAccessor.for_block(block).partition_by_key_hash(keys, n_partitions)
        metadata = BlockAccessor.for_block(block).get_metadata(
            input_files=None, exec_stats=BlockExecStats.builder().build()
        )
        return metadata, partitions

    def execute(
        self,
        sources: List[Tuple[List[str], BlockList]],
        n_partitions: int,
        clear_input_blocks: bool,
        *,
        map_ray_remote_args: Optional[Dict[str, Any]] = None,
        reduce_ray_remote_args: Optional[Dict[str, Any]] = None,
        ctx: Optional[TaskContext] = None,
    ) -> Tuple[BlockList, Dict[str, List[BlockMetadata]]]:
        print('here')
        source_input_block_sizes = [
            len(source_input_block[1].get_blocks())
            for source_input_block in sources
        ]
        n_sources = len(sources)
        source_keys = [keys for keys, _ in sources]
        total_input_blocks = sum(source_input_block_sizes)
        if map_ray_remote_args is None:
            map_ray_remote_args = {}
        if reduce_ray_remote_args is None:
            reduce_ray_remote_args = {}
        if "scheduling_strategy" not in reduce_ray_remote_args:
            reduce_ray_remote_args = reduce_ray_remote_args.copy()
            reduce_ray_remote_args["scheduling_strategy"] = "SPREAD"

        shuffle_map = cached_remote_fn(self.map)
        shuffle_reduce = cached_remote_fn(self.reduce)

        # should_close_bar = True
        should_close_bar = False
        if ctx is not None and ctx.sub_progress_bar_dict is not None:
            bar_name = "CogroupMap"
            assert bar_name in ctx.sub_progress_bar_dict, ctx.sub_progress_bar_dict
            map_bar = ctx.sub_progress_bar_dict[bar_name]
            should_close_bar = False
        else:
            map_bar = ProgressBar("Shuffle Map", total=total_input_blocks)

        shuffle_map_out = [
            shuffle_map.options(
                **map_ray_remote_args
            ).remote(i, block, keys, n_partitions, *self._map_args)
            for keys, input_blocks_list in sources
            for i, block in enumerate(input_blocks_list.get_blocks())
        ]

        in_blocks_owned_by_consumer = [
            input_blocks._owned_by_consumer for _, input_blocks in sources]

        if clear_input_blocks:
            for input_blocks in sources:
                input_blocks.clear()

        shuffle_map_out = map_bar.fetch_until_complete(shuffle_map_out)
        if should_close_bar:
            map_bar.close()

        # partition, source, n
        outputs: List[List[List[Block]]] = [[[] for _ in range(n_sources)] for _ in range(n_partitions)]
        # source, n
        map_metadata: List[List[BlockMetadata]] = []
        idx = 0
        for source, source_input_size in enumerate(source_input_block_sizes):
            source_metadata = []
            # Tuple[BlockMetadata, List[Block]]:
            for block_metadata, partitions in shuffle_map_out[idx:idx+source_input_size]:
                source_metadata.append(block_metadata)
                for partition_idx, partition in enumerate(partitions):
                    outputs[partition_idx][source].append(partition)
            map_metadata.append(source_metadata)
            idx += source_input_size

        # for source, source_input_size in enumerate(source_input_block_sizes):
        #     for partition_idx in range(n_partitions):
                # print(f"p:{partition_idx}, s:{source} \n {outputs[partition_idx][source]}")

        should_close_bar = True
        if ctx is not None and ctx.sub_progress_bar_dict is not None:
            bar_name = "ShuffleReduce"
            assert bar_name in ctx.sub_progress_bar_dict, ctx.sub_progress_bar_dict
            reduce_bar = ctx.sub_progress_bar_dict[bar_name]
            should_close_bar = False
        else:
            reduce_bar = ProgressBar("Shuffle Reduce", total=n_partitions)

        shuffle_reduce_out = [
            shuffle_reduce.options(**reduce_ray_remote_args, num_returns=2).remote(
                source_keys,
                outputs[partition_idx],
            )
            for partition_idx in range(n_partitions)
        ]

        # print(f"blah {len(shuffle_reduce_out)}")

        # shuffle_reduce_out = []
        # for partition_idx in range(n_partitions):
        #     res = shuffle_reduce.options(**reduce_ray_remote_args).remote(
        #         source_keys,
        #         outputs[partition_idx],
        #     )
        #     shuffle_reduce_out.append(res)

        new_blocks, new_metadata = zip(*shuffle_reduce_out)
        new_metadata = reduce_bar.fetch_until_complete(list(new_metadata))

        if should_close_bar:
            reduce_bar.close()

        stats = {
            "map": map_metadata,
            "reduce": new_metadata,
        }

        return (
            BlockList(
                list(new_blocks),
                list(new_metadata),
                owned_by_consumer=in_blocks_owned_by_consumer,
            ),
            stats,
        )



        #
        # if should_close_bar:
        #     reduce_bar.close()

        # shuffle_reduce_out = [
        #     shuffle_reduce.options(**reduce_ray_remote_args, num_returns=2,).remote(
        #         *self._reduce_args,
        #         *[shuffle_map_out[i][j] for i in range(input_num_blocks)],
        #     )
        #     for j in range(output_num_blocks)
        # ]
        # # Eagerly delete the map block references in order to eagerly release
        # # the blocks' memory.
        # del shuffle_map_out
        # new_blocks, new_metadata = zip(*shuffle_reduce_out)
        # new_metadata = reduce_bar.fetch_until_complete(list(new_metadata))
        #
        # if should_close_bar:
        #     reduce_bar.close()
        #
        # stats = {
        #     "map": shuffle_map_metadata,
        #     "reduce": new_metadata,
        # }
        #
        # return (
        #     BlockList(
        #         list(new_blocks),
        #         list(new_metadata),
        #         owned_by_consumer=in_blocks_owned_by_consumer,
        #     ),
        #     stats,
        # )

class SimpleJoin(JoinOp, SimpleCoGroupPlan):
    pass