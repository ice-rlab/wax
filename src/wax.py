import polars as pl
import polars.selectors as cs
import polars_ds as pds
import sys
from path_mapper import replace_with_path, map_path_by_name
from function_mapper import map_func_by_debug
from source_mapper import map_src, map_src_with_func
from instruction_mapper import map_ins
from basicblock_mapper import map_bb
from utils import (
    read_yaml_func, read_paths, read_func_info, read_cfgs, read_srcs, read_ins, 
    levenshtein_similarity_expr
)

def wax(old_log_path, new_log_path, 
        old_src_path, new_src_path, 
        old_debug_path, new_debug_path, 
        common_paths, yaml_in_path,
        func_map_out_path, bb_map_out_path, bb_cross_out_path, map_all_src=False):
    old_func_df, old_node_df, old_edge_df = read_cfgs(old_log_path)
    old_yaml_func_df = read_yaml_func(yaml_in_path).join(old_func_df, on="func")
    
    old_path_df = read_paths([old_src_path, *common_paths])
    old_func_info_df = replace_with_path(
        read_func_info(old_log_path), "file", old_path_df
    ).drop("fid").join(old_func_df, on="func")
    if map_all_src:
        old_node_select_df = old_node_df
    else:
        old_node_select_df = old_node_df.join(old_yaml_func_df.select("fid"), on="fid")
    old_ins_df = replace_with_path(
        read_ins(old_debug_path, old_node_select_df.lazy()).collect(), 
        "file", old_path_df
    )

    new_func_df, new_node_df, new_edge_df = read_cfgs(new_log_path)
    new_path_df = read_paths([new_src_path, *common_paths])
    new_func_info_df = replace_with_path(
        read_func_info(new_log_path), "file", new_path_df
    ).drop("fid").join(new_func_df, on="func")
    new_ins_df = replace_with_path(
        read_ins(new_debug_path, new_node_df.lazy()).collect(), 
        "file", new_path_df
    )

    func_map_df = map_func_by_debug(old_func_info_df, new_func_info_df, old_yaml_func_df)

    if not map_all_src:
        new_ins_df = new_ins_df.join(
            pl.concat([
                func_map_df.select(pl.col("new_fid").alias("fid")),
                map_path_by_name(
                    old_ins_df.select("path").unique(), 
                    new_ins_df.select("path").unique()
                )
                .select(pl.col("new_path").alias("path"))
                .join(new_ins_df.select("fid", "path").unique(), on="path")
                .select("fid")
            ])
            .unique(),
            on="fid",
        )

    old_src_df = read_srcs(old_ins_df["path"].unique())
    new_src_df = read_srcs(new_ins_df["path"].unique())

    src_map_df = map_src(old_src_df, new_src_df, 
                         old_ins_df.select("path", "line").unique(), 
                         new_ins_df.select("path", "line").unique())

    temp_match_df = (
        src_map_df.join(
            old_ins_df.select(cs.by_name("fid", "path", "line").name.prefix("old_")).unique()
            .join(func_map_df, on="old_fid", how="anti"),
            on=["old_path", "old_line"],
        ).join(
            new_ins_df.select(cs.by_name("fid", "path", "line").name.prefix("new_")).unique()
            .join(func_map_df, on="new_fid", how="anti"),
            on=["new_path", "new_line"],
        )
        .join(old_src_df, left_on=["old_path", "old_line"], right_on=["path", "line"])
        .join(new_src_df, left_on=["new_path", "new_line"], right_on=["path", "line"])
        .with_columns(
            pds.str_leven("code", "code_right", True, False).alias("distance")
        )
        .with_columns(
            lcs=(pl.col("code").str.len_chars() + 
                 pl.col("code_right").str.len_chars() - 
                 pl.col("distance")) // 2
        )
        .group_by("old_fid", "new_fid")
        .agg(pl.sum("lcs").alias("score"))
    )

    temp_map_dfs: list[pl.DataFrame] = []
    while True:
        temp_map_df = (
            temp_match_df
            .filter(
                pl.col("score").max().over("old_fid").eq(pl.col("score")) &
                pl.col("score").max().over("new_fid").eq(pl.col("score"))
            )
            .filter(pl.col("old_fid").is_unique() & pl.col("new_fid").is_unique())
        )
        temp_map_dfs.append(temp_map_df)
        if temp_map_df.is_empty():
            break
        temp_match_df = (
            temp_match_df
            .join(temp_map_df, on="old_fid", how="anti")
            .join(temp_map_df, on="new_fid", how="anti")
        )

    while True:
        temp_map_df = (
            temp_match_df
            .filter(
                pl.col("score").max().over("old_fid").eq(pl.col("score")) &
                pl.col("score").max().over("new_fid").eq(pl.col("score"))
            )
            .join(old_yaml_func_df.rename(lambda col: "old_" + col), on="old_fid")
            .join(old_func_info_df.rename(lambda col: "old_" + col), on=["old_fid", "old_func"])
            .join(new_func_info_df.rename(lambda col: "new_" + col), on="new_fid")
            .with_columns(
                pl.struct(
                    funcname=(
                        levenshtein_similarity_expr("old_namespace", "new_namespace") +
                        levenshtein_similarity_expr("old_basename", "new_basename")
                    ),
                    parameters=levenshtein_similarity_expr("old_parameters", "new_parameters"),
                    extension=levenshtein_similarity_expr("old_extension", "new_extension"),
                    chunk=levenshtein_similarity_expr("old_chunk", "new_chunk"),
                    len=-1*(pl.col("old_func").str.len_chars().cast(int) - 
                            pl.col("new_func").str.len_chars()).abs()
                ).alias("similarity")
            )
            .with_columns(pl.col("similarity").rank("min", descending=True).alias("rank"))
            .filter(
                pl.col("rank").min().over("old_func").eq(pl.col("rank")) &
                pl.col("rank").min().over("new_func").eq(pl.col("rank"))
            )
            .sort("old_func", "new_func")
            .group_by("old_fid")
            .first()
            .sort("old_func", "new_func")
            .group_by("new_fid")
            .first()
            .select("old_fid", "new_fid")
        )
        if temp_map_df.is_empty():
            break
        temp_match_df = (
            temp_match_df
            .join(temp_map_df, on="old_fid", how="anti")
            .join(temp_map_df, on="new_fid", how="anti")
        )

    func_map_df.vstack(
        pl.concat(temp_map_dfs)
        .join(old_yaml_func_df.rename(lambda col: "old_" + col), on="old_fid")
        .join(new_func_info_df.rename(lambda col: "new_" + col), on="new_fid")
        .group_by("old_fid", "new_fid")
        .agg(pl.first("old_func", "new_func"))
        .select("old_fid", "old_func", "new_fid", "new_func"),
        in_place=True
    )

    multi_func_map_df = func_map_df.vstack(
        pl.concat(
            src_map_df
            .join(
                old_ins_df.select(cs.by_name("fid", "path", "line").name.prefix("old_")).unique(), 
                on=["old_path", "old_line"],
            )
            .join(
                new_ins_df.select(cs.by_name("fid", "path", "line").name.prefix("new_")).unique(), 
                on=["new_path", "new_line"],
            )
            .join(old_src_df, left_on=["old_path", "old_line"], right_on=["path", "line"])
            .join(new_src_df, left_on=["new_path", "new_line"], right_on=["path", "line"])
            .with_columns(
                pds.str_leven("code", "code_right", True, False).alias("distance")
            )
            .with_columns(
                lcs=(pl.col("code").str.len_chars() + 
                     pl.col("code_right").str.len_chars() - 
                     pl.col("distance")) // 2
            )
            .group_by("old_fid", "new_fid")
            .agg(pl.sum("lcs").alias("score"))
            .join(
                func_map_df.with_columns(pl.lit(True).alias("has_map")), 
                on=["old_fid", "new_fid"], how="left",
            )
            .fill_null(False)
            .with_columns(
                map_score=(pl.col("has_map") * pl.col("score")).sum().over(f"{s}_fid")
            )
            .filter(
                pl.col("score").gt(pl.col("map_score")) &
                pl.col("score").max().over(f"{s}_fid").eq(pl.col("score")) &
                ~pl.col("has_map")
            )
            .filter(
                pl.col("score").max().over(f"{o}_fid").eq(pl.col("score"))
            )
            .filter(pl.col("old_fid").is_unique() & pl.col("new_fid").is_unique())
            .select("old_fid", "new_fid", "score")
            for s, o in [("old", "new"), ("new", "old")]
        )
        .unique()
        .filter(
            pl.col("score").max().over("old_fid").eq(pl.col("score")) & 
            pl.col("score").max().over("new_fid").eq(pl.col("score"))
        )
        .filter(pl.col("old_fid").is_unique() & pl.col("new_fid").is_unique())
        .drop("score")
        .join(old_yaml_func_df.rename(lambda col: "old_" + col), on="old_fid")
        .join(new_func_info_df.rename(lambda col: "new_" + col), on="new_fid")
        .group_by("old_fid", "new_fid")
        .agg(pl.first("old_func", "new_func"))
        .select("old_fid", "old_func", "new_fid", "new_func")
    )

    fsrc_map_df = map_src_with_func(
        old_ins_df.select("fid", "path", "line").unique().join(old_src_df, on=["path", "line"]),
        new_ins_df.select("fid", "path", "line").unique().join(new_src_df, on=["path", "line"]),
        multi_func_map_df, src_map_df, map_remaining=True
    )
    ins_map_df = map_ins(old_ins_df, new_ins_df, fsrc_map_df)
    bb_map_df = map_bb(old_node_df, new_node_df, old_edge_df, new_edge_df, ins_map_df, func_map_df)

    updated_func_map_df = (
        bb_map_df.group_by("old_fid", "new_fid").len()
        .filter(
            pl.col("len").max().over("old_fid").eq(pl.col("len")) &
            pl.col("len").max().over("new_fid").eq(pl.col("len"))
        )
        .join(
            func_map_df.select("old_fid", "new_fid", pl.lit(True).alias("is_map")), 
            on=["old_fid", "new_fid"], how="left",
        )
        .filter(pl.col("is_map").max().over("old_fid").eq_missing(pl.col("is_map")))
        .filter(pl.col("is_map").is_null())
        .join(func_map_df.select("old_fid", "old_func"), on="old_fid")
        .join(new_func_df.rename(lambda col: "new_" + col), on="new_fid", how="left")
        .with_columns(
            pds.str_leven("old_func", "new_func", True, True).alias("similarity")
        )
        .filter(pl.col("similarity").max().over("old_fid").eq(pl.col("similarity")))
        .sort("new_func")
        .group_by("old_fid", "old_func", "new_fid")
        .agg(pl.first("new_func"))
        .join(func_map_df, on=["old_fid", "old_func"], how="right")
        .select(
            "old_fid", "old_func",
            pl.col("new_fid").fill_null(pl.col("new_fid_right")),
            pl.col("new_func").fill_null(pl.col("new_func_right"))
        )
    )

    (
        updated_func_map_df
        .select("old_func", "new_func")
        .write_csv(func_map_out_path, include_header=False)
    )

    (
        bb_map_df.join(updated_func_map_df, on=["old_fid", "new_fid"])
        .select(
            pl.col("old_func").alias("yaml_bf"), 
            pl.col("old_bid").alias("yaml_bb"), 
            pl.col("new_bid").alias("binary_bb"),
        )
        .sort("yaml_bf", "yaml_bb")
        .write_csv(bb_map_out_path, include_header=False)
    )

    (
        bb_map_df.join(updated_func_map_df, on=["old_fid", "new_fid"], how="anti")
        .join(multi_func_map_df, on=["old_fid", "new_fid"])
        .select("new_func", "new_bid", "old_func", "old_bid")
        .sort("new_func", "new_bid", "old_func", "old_bid")
        .write_csv(bb_cross_out_path, include_header=False)
    )

if __name__ == "__main__":
    old_log_path = sys.argv[1]
    new_log_path = sys.argv[2]
    old_src_path = sys.argv[3]
    new_src_path = sys.argv[4]
    old_debug_path = sys.argv[5]
    new_debug_path = sys.argv[6]
    common_paths = ["/usr/include", "/usr/lib/gcc/x86_64-linux-gnu"]
    yaml_in_path = sys.argv[7]
    func_map_out_path = sys.argv[8]
    bb_map_out_path = sys.argv[9]
    bb_cross_out_path = sys.argv[10]
    map_all_src = (len(sys.argv) > 11 and sys.argv[11] == 'y')

    wax(old_log_path, new_log_path, 
        old_src_path, new_src_path, 
        old_debug_path, new_debug_path, 
        common_paths, yaml_in_path,
        func_map_out_path, bb_map_out_path, bb_cross_out_path, map_all_src)
