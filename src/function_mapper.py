import numpy as np
import polars as pl
from path_mapper import replace_with_path
from utils import levenshtein_similarity_expr

def update_func_map(match_df: pl.DataFrame):
    func_map_df = match_df.select(
        "old_fid", "old_func", "old_path", "old_line", 
        "new_fid", "new_func", "new_path", "new_line",
    ).sample(0)
    match_df = match_df.with_columns(pl.col("similarity").rank("min", descending=True).alias("rank"))
    while not match_df.is_empty():
        map_df = (
            match_df
            .filter(
                pl.col("rank").min().over("old_fid").eq(pl.col("rank")) &
                pl.col("rank").min().over("new_fid").eq(pl.col("rank"))
            )
            .sort("old_func", "new_func")
            .group_by("old_fid")
            .first()
            .sort("old_func", "new_func")
            .group_by("new_fid")
            .first()
            .select("old_fid", "old_func", "old_path", "old_line", 
                    "new_fid", "new_func", "new_path", "new_line")
        )
        match_df = (
            match_df
            .join(map_df, on="new_fid", how="anti")
            .join(map_df, on="old_fid", how="anti")
        )
        func_map_df.vstack(map_df, in_place=True)
    
    return func_map_df

def map_func_internal(old_func_info_df: pl.DataFrame, new_func_info_df: pl.DataFrame, 
                      old_yaml_func_df: pl.DataFrame):
    # declare collection to keep incrementally computed function maps
    func_map_dfs: list[pl.DataFrame] = []

    old_column_exprs = old_func_info_df.columns
    new_column_exprs = [pl.col(col + "_right").alias(col) for col in new_func_info_df.columns]
    rename_columns = {
        "fid": "old_fid", "func": "old_func", "path": "old_path", "line": "old_line", 
        "fid_right": "new_fid", "func_right": "new_func", "path_right": "new_path", "line_right": "new_line", 
    }

    full_df = old_func_info_df.join(new_func_info_df, on=["func"], how="full")
    old_func_info_df = full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_null()).select(old_column_exprs)
    new_func_info_df = full_df.filter(pl.col("func").is_null() & pl.col("func_right").is_not_null()).select(new_column_exprs)

    df = (
        full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_not_null())
        # .join(old_yaml_func_df.select("func"), on="func")
        .rename(rename_columns)
        .with_columns(pl.lit(1).alias("similarity"))
    )

    map_df = update_func_map(df)
    old_func_info_df = old_func_info_df.join(map_df, left_on="fid", right_on="old_fid", how="anti")
    new_func_info_df = new_func_info_df.join(map_df, left_on="fid", right_on="new_fid", how="anti")
    func_map_dfs.append(map_df)

    # match: "namespace", "basename", "parameters", "extension", "chunk"
    # maximize similarity: "len"
    full_df = old_func_info_df.join(new_func_info_df, on=["namespace", "basename", "parameters", "extension", "chunk"], how="full")
    old_func_info_df = full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_null()).select(old_column_exprs)
    new_func_info_df = full_df.filter(pl.col("func").is_null() & pl.col("func_right").is_not_null()).select(new_column_exprs)

    df = (
        full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_not_null())
        # .join(old_yaml_func_df.select("func"), on="func")
        .rename(rename_columns)
        .with_columns(
            (-1*(pl.col("old_func").str.len_chars().cast(int) - pl.col("new_func").str.len_chars()).abs())
            .alias("similarity")
        )
    )

    map_df = update_func_map(df)
    old_func_info_df = old_func_info_df.join(map_df, left_on="fid", right_on="old_fid", how="anti")
    new_func_info_df = new_func_info_df.join(map_df, left_on="fid", right_on="new_fid", how="anti")
    func_map_dfs.append(map_df)

    # match: "namespace", "basename", "parameters", "extension"
    # maximize similarity: "chunk", "len"
    full_df = old_func_info_df.join(new_func_info_df, on=["namespace", "basename", "parameters", "extension"], how="full")
    old_func_info_df = full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_null()).select(old_column_exprs)
    new_func_info_df = full_df.filter(pl.col("func").is_null() & pl.col("func_right").is_not_null()).select(new_column_exprs)

    df = (
        full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_not_null())
        .join(old_yaml_func_df.select("func"), on="func")
        .rename(rename_columns)
        .with_columns(
            pl.struct(
                chunk=levenshtein_similarity_expr("chunk", "chunk_right"),
                len=-1*(pl.col("old_func").str.len_chars().cast(int) - pl.col("new_func").str.len_chars()).abs()
            ).alias("similarity")
        )
    )

    map_df = update_func_map(df)
    old_func_info_df = old_func_info_df.join(map_df, left_on="fid", right_on="old_fid", how="anti")
    new_func_info_df = new_func_info_df.join(map_df, left_on="fid", right_on="new_fid", how="anti")
    func_map_dfs.append(map_df)

    # match: "namespace", "basename", "parameters"
    # maximize similarity: "extension", "chunk", "len"
    full_df = old_func_info_df.join(new_func_info_df, on=["namespace", "basename", "parameters"], how="full")
    old_func_info_df = full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_null()).select(old_column_exprs)
    new_func_info_df = full_df.filter(pl.col("func").is_null() & pl.col("func_right").is_not_null()).select(new_column_exprs)

    df = (
        full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_not_null())
        .join(old_yaml_func_df.select("func"), on="func")
        .filter(pl.col("extension").str.contains("cold") == pl.col("extension_right").str.contains("cold"))
        .rename(rename_columns)
        .with_columns(
            pl.struct(
                extension=levenshtein_similarity_expr("extension", "extension_right"),
                chunk=levenshtein_similarity_expr("chunk", "chunk_right"),
                len=-1*(pl.col("old_func").str.len_chars().cast(int) - pl.col("new_func").str.len_chars()).abs()
            ).alias("similarity")
        )
    )

    map_df = update_func_map(df)
    old_func_info_df = old_func_info_df.join(map_df, left_on="fid", right_on="old_fid", how="anti")
    new_func_info_df = new_func_info_df.join(map_df, left_on="fid", right_on="new_fid", how="anti")
    func_map_dfs.append(map_df)
    
    # match: "path", "namespace", "basename"
    # maximize similarity: "parameters", "extension", "chunk", "len"
    full_df = old_func_info_df.join(new_func_info_df, on=["path", "namespace", "basename"], how="full")
    old_func_info_df = full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_null()).select(old_column_exprs)
    new_func_info_df = full_df.filter(pl.col("func").is_null() & pl.col("func_right").is_not_null()).select(new_column_exprs)

    df = (
        full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_not_null())
        .join(old_yaml_func_df.select("func"), on="func")
        .filter(pl.col("extension").str.contains("cold") == pl.col("extension_right").str.contains("cold"))
        .rename(rename_columns)
        .with_columns(
            pl.struct(
                parameters=levenshtein_similarity_expr("parameters", "parameters_right"),
                extension=levenshtein_similarity_expr("extension", "extension_right"),
                chunk=levenshtein_similarity_expr("chunk", "chunk_right"),
                len=-1*(pl.col("old_func").str.len_chars().cast(int) - pl.col("new_func").str.len_chars()).abs()
            ).alias("similarity")
        )
    )

    map_df = update_func_map(df)
    old_func_info_df = old_func_info_df.join(map_df, left_on="fid", right_on="old_fid", how="anti")
    new_func_info_df = new_func_info_df.join(map_df, left_on="fid", right_on="new_fid", how="anti")
    func_map_dfs.append(map_df)
    
    # match: "namespace", "basename"
    # maximize similarity: "parameters", "extension", "chunk", "len"
    full_df = old_func_info_df.join(new_func_info_df, on=["namespace", "basename"], how="full")
    old_func_info_df = full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_null()).select(old_column_exprs)
    new_func_info_df = full_df.filter(pl.col("func").is_null() & pl.col("func_right").is_not_null()).select(new_column_exprs)

    df = (
        full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_not_null())
        .join(old_yaml_func_df.select("func"), on="func")
        .filter(pl.col("extension").str.contains("cold") == pl.col("extension_right").str.contains("cold"))
        .rename(rename_columns)
        .with_columns(
            pl.struct(
                parameters=levenshtein_similarity_expr("parameters", "parameters_right"),
                extension=levenshtein_similarity_expr("extension", "extension_right"),
                chunk=levenshtein_similarity_expr("chunk", "chunk_right"),
                len=-1*(pl.col("old_func").str.len_chars().cast(int) - pl.col("new_func").str.len_chars()).abs()
            ).alias("similarity")
        )
    )

    map_df = update_func_map(df)
    old_func_info_df = old_func_info_df.join(map_df, left_on="fid", right_on="old_fid", how="anti")
    new_func_info_df = new_func_info_df.join(map_df, left_on="fid", right_on="new_fid", how="anti")
    func_map_dfs.append(map_df)

    # match: "path", "basename", "parameters"
    # maximize similarity: "namespace", "extension", "chunk", "len"
    full_df = old_func_info_df.join(new_func_info_df, on=["path", "basename", "parameters"], how="full")
    old_func_info_df = full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_null()).select(old_column_exprs)
    new_func_info_df = full_df.filter(pl.col("func").is_null() & pl.col("func_right").is_not_null()).select(new_column_exprs)

    df = (
        full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_not_null())
        .join(old_yaml_func_df.select("func"), on="func")
        .filter(pl.col("extension").str.contains("cold") == pl.col("extension_right").str.contains("cold"))
        .rename(rename_columns)
        .with_columns(
            pl.struct(
                namespace=levenshtein_similarity_expr("namespace", "namespace_right"),
                extension=levenshtein_similarity_expr("extension", "extension_right"),
                chunk=levenshtein_similarity_expr("chunk", "chunk_right"),
                len=-1*(pl.col("old_func").str.len_chars().cast(int) - pl.col("new_func").str.len_chars()).abs()
            ).alias("similarity")
        )
    )

    map_df = update_func_map(df)
    old_func_info_df = old_func_info_df.join(map_df, left_on="fid", right_on="old_fid", how="anti")
    new_func_info_df = new_func_info_df.join(map_df, left_on="fid", right_on="new_fid", how="anti")
    func_map_dfs.append(map_df)

    # match: "basename", "parameters"
    # maximize similarity: "namespace", "extension", "chunk", "len"
    full_df = old_func_info_df.join(new_func_info_df, on=["basename", "parameters"], how="full")
    old_func_info_df = full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_null()).select(old_column_exprs)
    new_func_info_df = full_df.filter(pl.col("func").is_null() & pl.col("func_right").is_not_null()).select(new_column_exprs)

    df = (
        full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_not_null())
        .join(old_yaml_func_df.select("func"), on="func")
        .filter(pl.col("extension").str.contains("cold") == pl.col("extension_right").str.contains("cold"))
        .rename(rename_columns)
        .with_columns(
            pl.struct(
                namespace=levenshtein_similarity_expr("namespace", "namespace_right"),
                extension=levenshtein_similarity_expr("extension", "extension_right"),
                chunk=levenshtein_similarity_expr("chunk", "chunk_right"),
                len=-1*(pl.col("old_func").str.len_chars().cast(int) - pl.col("new_func").str.len_chars()).abs()
            ).alias("similarity")
        )
    )

    map_df = update_func_map(df)
    old_func_info_df = old_func_info_df.join(map_df, left_on="fid", right_on="old_fid", how="anti")
    new_func_info_df = new_func_info_df.join(map_df, left_on="fid", right_on="new_fid", how="anti")
    func_map_dfs.append(map_df)
    
    # take multiple paths from here
    temp_map_dfs: list[pl.DataFrame] = []

    old_func_info_remain_df = old_func_info_df
    new_func_info_remain_df = new_func_info_df
    
    # path 1
    # match: "path", "basename"
    # maximize similarity: "namespace", "parameters", "extension", "chunk", "len"
    full_df = old_func_info_df.join(new_func_info_df, on=["path", "basename"], how="full")
    old_func_info_df = full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_null()).select(old_column_exprs)
    new_func_info_df = full_df.filter(pl.col("func").is_null() & pl.col("func_right").is_not_null()).select(new_column_exprs)

    df = (
        full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_not_null())
        .join(old_yaml_func_df.select("func"), on="func")
        .filter(pl.col("extension").str.contains("cold") == pl.col("extension_right").str.contains("cold"))
        .rename(rename_columns)
        .with_columns(
            pl.struct(
                namespace=levenshtein_similarity_expr("namespace", "namespace_right"),
                parameters=levenshtein_similarity_expr("parameters", "parameters_right"),
                extension=levenshtein_similarity_expr("extension", "extension_right"),
                chunk=levenshtein_similarity_expr("chunk", "chunk_right"),
                len=-1*(pl.col("old_func").str.len_chars().cast(int) - pl.col("new_func").str.len_chars()).abs()
            ).alias("similarity")
        )
    )

    map_df = update_func_map(df)
    old_func_info_df = old_func_info_df.join(map_df, left_on="fid", right_on="old_fid", how="anti")
    new_func_info_df = new_func_info_df.join(map_df, left_on="fid", right_on="new_fid", how="anti")
    temp_map_dfs.append(map_df)
    
    # match: "basename"
    # maximize similarity: "namespace", "parameters", "extension", "chunk", "len"
    full_df = old_func_info_df.join(new_func_info_df, on=["basename"], how="full")
    old_func_info_df = full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_null()).select(old_column_exprs)
    new_func_info_df = full_df.filter(pl.col("func").is_null() & pl.col("func_right").is_not_null()).select(new_column_exprs)

    df = (
        full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_not_null())
        .join(old_yaml_func_df.select("func"), on="func")
        .filter(pl.col("extension").str.contains("cold") == pl.col("extension_right").str.contains("cold"))
        .rename(rename_columns)
        .with_columns(
            pl.struct(
                namespace=levenshtein_similarity_expr("namespace", "namespace_right"),
                parameters=levenshtein_similarity_expr("parameters", "parameters_right"),
                extension=levenshtein_similarity_expr("extension", "extension_right"),
                chunk=levenshtein_similarity_expr("chunk", "chunk_right"),
                len=-1*(pl.col("old_func").str.len_chars().cast(int) - pl.col("new_func").str.len_chars()).abs()
            ).alias("similarity")
        )
    )

    map_df = update_func_map(df)
    old_func_info_df = old_func_info_df.join(map_df, left_on="fid", right_on="old_fid", how="anti")
    new_func_info_df = new_func_info_df.join(map_df, left_on="fid", right_on="new_fid", how="anti")
    temp_map_dfs.append(map_df)

    # path 2
    old_func_info_df = old_func_info_remain_df
    new_func_info_df = new_func_info_remain_df
    
    # match: "path", "namespace"
    # maximize similarity: "basename", "parameters", "extension", "chunk", "len"
    full_df = old_func_info_df.join(new_func_info_df, on=["path", "namespace"], how="full")
    old_func_info_df = full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_null()).select(old_column_exprs)
    new_func_info_df = full_df.filter(pl.col("func").is_null() & pl.col("func_right").is_not_null()).select(new_column_exprs)

    df = (
        full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_not_null())
        .join(old_yaml_func_df.select("func"), on="func")
        .filter(pl.col("extension").str.contains("cold") == pl.col("extension_right").str.contains("cold"))
        .rename(rename_columns)
        .with_columns(
            pl.struct(
                basename=levenshtein_similarity_expr("basename", "basename_right"),
                parameters=levenshtein_similarity_expr("parameters", "parameters_right"),
                extension=levenshtein_similarity_expr("extension", "extension_right"),
                chunk=levenshtein_similarity_expr("chunk", "chunk_right"),
                len=-1*(pl.col("old_func").str.len_chars().cast(int) - pl.col("new_func").str.len_chars()).abs()
            ).alias("similarity")
        )
    )

    map_df = update_func_map(df)
    old_func_info_df = old_func_info_df.join(map_df, left_on="fid", right_on="old_fid", how="anti")
    new_func_info_df = new_func_info_df.join(map_df, left_on="fid", right_on="new_fid", how="anti")
    temp_map_dfs.append(map_df)

    # match: "path"
    # maximize similarity: "namespace", "basename", "parameters", "extension", "chunk", "len"
    full_df = old_func_info_df.join(new_func_info_df, on=["path"], how="full")
    old_func_info_df = full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_null()).select(old_column_exprs)
    new_func_info_df = full_df.filter(pl.col("func").is_null() & pl.col("func_right").is_not_null()).select(new_column_exprs)

    df = (
        full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_not_null())
        .join(old_yaml_func_df.select("func"), on="func")
        .filter(pl.col("extension").str.contains("cold") == pl.col("extension_right").str.contains("cold"))
        .rename(rename_columns)
        .with_columns(
            pl.struct(
                funcname=(
                    levenshtein_similarity_expr("namespace", "namespace_right") +
                    levenshtein_similarity_expr("basename", "basename_right")
                ),
                parameters=levenshtein_similarity_expr("parameters", "parameters_right"),
                extension=levenshtein_similarity_expr("extension", "extension_right"),
                chunk=levenshtein_similarity_expr("chunk", "chunk_right"),
                len=-1*(pl.col("old_func").str.len_chars().cast(int) - pl.col("new_func").str.len_chars()).abs()
            ).alias("similarity")
        )
    )

    map_df = update_func_map(df)
    old_func_info_df = old_func_info_df.join(map_df, left_on="fid", right_on="old_fid", how="anti")
    new_func_info_df = new_func_info_df.join(map_df, left_on="fid", right_on="new_fid", how="anti")
    temp_map_dfs.append(map_df)

    # convert: "path" -> "filename"
    old_func_info_df = old_func_info_df.with_columns(pl.col("path").str.replace(r".*/", "").alias("file"))
    new_func_info_df = new_func_info_df.with_columns(pl.col("path").str.replace(r".*/", "").alias("file"))

    # match: "filename"
    # maximize similarity: "namespace", "basename", "parameters", "extension", "chunk", "len"
    full_df = old_func_info_df.join(new_func_info_df, on=["file"], how="full")
    old_func_info_df = full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_null()).select(old_column_exprs)
    new_func_info_df = full_df.filter(pl.col("func").is_null() & pl.col("func_right").is_not_null()).select(new_column_exprs)

    df = (
        full_df.filter(pl.col("func").is_not_null() & pl.col("func_right").is_not_null())
        .join(old_yaml_func_df.select("func"), on="func")
        .filter(pl.col("extension").str.contains("cold") == pl.col("extension_right").str.contains("cold"))
        .rename(rename_columns)
        .with_columns(
            pl.struct(
                funcname=(
                    levenshtein_similarity_expr("namespace", "namespace_right") +
                    levenshtein_similarity_expr("basename", "basename_right")
                ),
                parameters=levenshtein_similarity_expr("parameters", "parameters_right"),
                extension=levenshtein_similarity_expr("extension", "extension_right"),
                chunk=levenshtein_similarity_expr("chunk", "chunk_right"),
                len=-1*(pl.col("old_func").str.len_chars().cast(int) - pl.col("new_func").str.len_chars()).abs()
            ).alias("similarity")
        )
    )

    map_df = update_func_map(df)
    old_func_info_df = old_func_info_df.join(map_df, left_on="fid", right_on="old_fid", how="anti")
    new_func_info_df = new_func_info_df.join(map_df, left_on="fid", right_on="new_fid", how="anti")
    temp_map_dfs.append(map_df)

    # # start with unmapped functions
    assert pl.concat(func_map_dfs).filter(pl.col("old_fid").is_duplicated() | pl.col("new_fid").is_duplicated()).is_empty()

    df = (
        pl.concat(temp_map_dfs)
        .join(old_func_info_remain_df, left_on="old_func", right_on="func")
        .join(new_func_info_remain_df, left_on="new_func", right_on="func")
        .with_columns(
            pl.struct(
                funcname=(
                    levenshtein_similarity_expr("namespace", "namespace_right") +
                    levenshtein_similarity_expr("basename", "basename_right")
                ),
                parameters=levenshtein_similarity_expr("parameters", "parameters_right"),
                extension=levenshtein_similarity_expr("extension", "extension_right"),
                chunk=levenshtein_similarity_expr("chunk", "chunk_right"),
                len=-1*(pl.col("old_func").str.len_chars().cast(int) - pl.col("new_func").str.len_chars()).abs()
            ).alias("similarity")
        )
    )

    map_df = update_func_map(df)
    old_func_info_df = old_func_info_df.join(map_df, left_on="fid", right_on="old_fid", how="anti")
    new_func_info_df = new_func_info_df.join(map_df, left_on="fid", right_on="new_fid", how="anti")
    func_map_dfs.append(map_df)

    return (
        pl.concat(func_map_dfs)
        .join(
            old_yaml_func_df, 
            left_on="old_fid", 
            right_on="fid",
        )
        .select("old_fid", pl.col("func").alias("old_func"), "new_fid", "new_func")
        .unique()
    )

def map_func_by_debug(old_func_info_df: pl.DataFrame, new_func_info_df: pl.DataFrame,
                      old_yaml_func_df: pl.DataFrame, try_all: bool = True):
    
    # replace path of old with corresponding new path so that we can directly map
    old_func_info_df = replace_with_path(
        old_func_info_df, "path",
        new_func_info_df.select("path").unique()
    )

    func_map_df = map_func_internal(old_func_info_df, new_func_info_df, old_yaml_func_df)

    while True:
        old_yaml_func_df = old_yaml_func_df.join(func_map_df, left_on="fid", right_on="old_fid", how="anti")
        old_func_info_df = old_func_info_df.join(old_yaml_func_df, on="fid", how="semi")
        new_func_info_df = new_func_info_df.join(func_map_df, left_on="fid", right_on="new_fid", how="anti")
        map_df = map_func_internal(old_func_info_df, new_func_info_df, old_yaml_func_df)
        if map_df.is_empty():
            break
        func_map_df.vstack(map_df, in_place=True)
    
    if not try_all:
        return func_map_df
    
    while True:
        df = (
            old_func_info_df.rename(lambda col: "old_" + col)
            .join(new_func_info_df.rename(lambda col: "new_" + col), how="cross")
            .with_columns(
                pl.struct(
                    funcname=(
                        levenshtein_similarity_expr("old_namespace", "new_namespace") +
                        levenshtein_similarity_expr("old_basename", "new_basename")
                    ),
                    parameters=levenshtein_similarity_expr("old_parameters", "new_parameters"),
                    extension=levenshtein_similarity_expr("old_extension", "new_extension"),
                    chunk=levenshtein_similarity_expr("old_chunk", "new_chunk"),
                    len=-1*(pl.col("old_func").str.len_chars().cast(int) - pl.col("new_func").str.len_chars()).abs()
                ).alias("similarity")
            )
        )
        if df.is_empty():
            break
        map_df = update_func_map(df).select(func_map_df.columns)
        old_func_info_df = old_func_info_df.join(map_df, left_on="fid", right_on="old_fid", how="anti")
        new_func_info_df = new_func_info_df.join(map_df, left_on="fid", right_on="new_fid", how="anti")
        func_map_df.vstack(map_df, in_place=True)
    
    return func_map_df
