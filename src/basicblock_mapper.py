import polars as pl

def join_edge(self: str, other: str, 
              bb_match_df: pl.DataFrame, bb_map_df: pl.DataFrame, 
              old_edge_df: pl.DataFrame, new_edge_df: pl.DataFrame) -> pl.DataFrame:
    match_edge_df = (
        bb_match_df
        .join(
            bb_map_df
            .join(
                old_edge_df.rename({
                    f"{other}_fid": f"{other}_old_fid", 
                    f"{other}_bid": f"{other}_old_bid"
                }), 
                left_on=["old_fid", "old_bid"], 
                right_on=[f"{self}_fid", f"{self}_bid"],
            )
            .join(
                new_edge_df.rename({
                    f"{other}_fid": f"{other}_new_fid", 
                    f"{other}_bid": f"{other}_new_bid"
                }), 
                left_on=["new_fid", "new_bid"], 
                right_on=[f"{self}_fid", f"{self}_bid"],
            ), 
            left_on=["old_fid", "old_bid", "new_fid", "new_bid"],
            right_on=[f"{other}_old_fid", f"{other}_old_bid", f"{other}_new_fid", f"{other}_new_bid"],
        )
        .group_by("old_fid", "old_bid", "new_fid", "new_bid")
        .len()
    )
    return match_edge_df

def map_bb(old_node_df: pl.DataFrame, new_node_df: pl.DataFrame,
           old_edge_df: pl.DataFrame, new_edge_df: pl.DataFrame,
           asm_map_df: pl.DataFrame, func_map_df: pl.DataFrame) -> pl.DataFrame:

    bb_match_df = (
        asm_map_df
        .group_by("old_fid", "old_bid", "new_fid", "new_bid")
        .agg(pl.sum("jump_func_score", "opcode_match_score", 
                    "jump_diff_score", "operand_match_score").round(6))
    )

    # bb_map_df = pl.DataFrame(
    #     schema=bb_match_df.select("old_fid", "old_bid", "new_fid", "new_bid").schema
    # )

    bb_map_df = (
        func_map_df
        .join(old_node_df, left_on="old_fid", right_on="fid")
        .join(new_node_df, left_on=["new_fid", "hash"], right_on=["fid", "hash"])
        .select("old_fid", pl.col("bid").alias("old_bid"), "new_fid", pl.col("bid_right").alias("new_bid"))
        .unique()
        .filter(pl.struct("old_fid", "old_bid").is_unique() & pl.struct("new_fid", "new_bid").is_unique())
    )
    bb_match_df = (
        bb_match_df
        .join(bb_map_df, on=["old_fid", "old_bid"], how="anti")
        .join(bb_map_df, on=["new_fid", "new_bid"], how="anti")
    )

    iter_count = 0
    while True:
        # print(iter_count, bb_match_df.shape[0], end="\r")
        iter_count += 1

        assert type(bb_match_df) == pl.DataFrame
        temp_map_df = (
            bb_match_df
            .filter(
                pl.col("jump_func_score").max().over("old_fid", "old_bid")
                .eq(pl.col("jump_func_score")) &
                pl.col("jump_func_score").max().over("new_fid", "new_bid")
                .eq(pl.col("jump_func_score"))
            )
            .filter(
                pl.col("opcode_match_score").max().over("old_fid", "old_bid")
                .eq(pl.col("opcode_match_score")) &
                pl.col("opcode_match_score").max().over("new_fid", "new_bid")
                .eq(pl.col("opcode_match_score"))
            )
            .filter(
                pl.struct("old_fid", "old_bid").is_unique() &
                pl.struct("new_fid", "new_bid").is_unique()
            )
            .select(bb_map_df.columns)
        )

        if not temp_map_df.is_empty():
            bb_map_df.vstack(temp_map_df, in_place=True)
            bb_match_df = (
                bb_match_df
                .join(temp_map_df, on=["old_fid", "old_bid"], how="anti")
                .join(temp_map_df, on=["new_fid", "new_bid"], how="anti")
            )
            continue

        match_edge1_df: pl.DataFrame = join_edge(
            "src", "dst", bb_match_df, bb_map_df, old_edge_df, new_edge_df,
        )
        temp_map1_df = (
            match_edge1_df
            .filter(
                pl.col("len").max().over("old_fid", "old_bid")
                .eq(pl.col("len")) &
                pl.col("len").max().over("new_fid", "new_bid")
                .eq(pl.col("len"))
            )
            .filter(
                pl.struct("old_fid", "old_bid").is_unique() &
                pl.struct("new_fid", "new_bid").is_unique()
            )
            .select(bb_map_df.columns)
        )

        bb_map_df.vstack(temp_map1_df, in_place=True)
        bb_match_df = (
            bb_match_df
            .join(temp_map1_df, on=["old_fid", "old_bid"], how="anti")
            .join(temp_map1_df, on=["new_fid", "new_bid"], how="anti")
        )

        match_edge2_df: pl.DataFrame = join_edge(
            "dst", "src", bb_match_df, bb_map_df, old_edge_df, new_edge_df,
        )
        temp_map2_df = (
            match_edge2_df
            .filter(
                pl.col("len").max().over("old_fid", "old_bid")
                .eq(pl.col("len")) &
                pl.col("len").max().over("new_fid", "new_bid")
                .eq(pl.col("len"))
            )
            .filter(
                pl.struct("old_fid", "old_bid").is_unique() &
                pl.struct("new_fid", "new_bid").is_unique()
            )
            .select(bb_map_df.columns)
        )

        bb_map_df.vstack(temp_map2_df, in_place=True)
        bb_match_df = (
            bb_match_df
            .join(temp_map2_df, on=["old_fid", "old_bid"], how="anti")
            .join(temp_map2_df, on=["new_fid", "new_bid"], how="anti")
        )

        if not temp_map1_df.is_empty() or not temp_map2_df.is_empty():
            continue

        match_edge_df = (
            pl.concat([match_edge1_df, match_edge2_df])
            .group_by("old_fid", "old_bid", "new_fid", "new_bid")
            .sum()
        )

        temp_map_df = (
            match_edge_df
            .filter(
                pl.col("len").max().over("old_fid", "old_bid")
                .eq(pl.col("len")) &
                pl.col("len").max().over("new_fid", "new_bid")
                .eq(pl.col("len"))
            )
            .filter(
                pl.struct("old_fid", "old_bid").is_unique() &
                pl.struct("new_fid", "new_bid").is_unique()
            )
            .select(bb_map_df.columns)
        )

        if not temp_map_df.is_empty():
            bb_map_df.vstack(temp_map_df, in_place=True)
            bb_match_df = (
                bb_match_df
                .join(temp_map_df, on=["old_fid", "old_bid"], how="anti")
                .join(temp_map_df, on=["new_fid", "new_bid"], how="anti")
            )
            continue
        
        temp_map_df = (
            bb_match_df
            .filter(
                pl.col("jump_func_score").max().over("old_fid", "old_bid")
                .eq(pl.col("jump_func_score")) &
                pl.col("jump_func_score").max().over("new_fid", "new_bid")
                .eq(pl.col("jump_func_score"))
            )
            .filter(
                pl.col("opcode_match_score").max().over("old_fid", "old_bid")
                .eq(pl.col("opcode_match_score")) &
                pl.col("opcode_match_score").max().over("new_fid", "new_bid")
                .eq(pl.col("opcode_match_score"))
            )
            .filter(
                pl.col("jump_diff_score").max().over("old_fid", "old_bid")
                .eq(pl.col("jump_diff_score")) &
                pl.col("jump_diff_score").max().over("new_fid", "new_bid")
                .eq(pl.col("jump_diff_score"))
            )
            .filter(
                pl.col("operand_match_score").max().over("old_fid", "old_bid")
                .eq(pl.col("operand_match_score")) &
                pl.col("operand_match_score").max().over("new_fid", "new_bid")
                .eq(pl.col("operand_match_score"))
            )
            .filter(
                pl.struct("old_fid", "old_bid").is_unique() &
                pl.struct("new_fid", "new_bid").is_unique()
            )
            .select(bb_map_df.columns)
        )
        
        if temp_map_df.is_empty():
            break

        bb_map_df.vstack(temp_map_df, in_place=True)
        bb_match_df = (
            bb_match_df
            .join(temp_map_df, on=["old_fid", "old_bid"], how="anti")
            .join(temp_map_df, on=["new_fid", "new_bid"], how="anti")
        )

    return bb_map_df
