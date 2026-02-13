import polars as pl
import polars_ds as pds

def map_path_by_name(old_path_df: pl.DataFrame, new_path_df: pl.DataFrame):
    old_names_df = (
        old_path_df
        .with_columns(pl.col("path").str.split("/").list.reverse().alias("name"))
        .with_columns(pl.int_ranges(0, pl.col("name").list.len()).alias("index"))
        .explode("name", "index")
    )

    new_names_df = (
        new_path_df
        .with_columns(pl.col("path").str.split("/").list.reverse().alias("name"))
        .with_columns(pl.int_ranges(0, pl.col("name").list.len()).alias("index"))
        .explode("name", "index")
    )

    i = 0
    match_df = (
        old_names_df.filter(pl.col("index") == i)
        .join(new_names_df.filter(pl.col("index") == i), on="name")
        .select("path", "path_right")
    )

    path_map_df = match_df.sample(0)

    while not match_df.is_empty():
        match_dfs = (
            match_df
            .with_columns(is_unique=pl.col("path").is_unique())
            .partition_by("is_unique", as_dict=True, include_key=False)
        )

        if (True,) in match_dfs:
            path_map_df.vstack(match_dfs[True,], in_place=True)
        if (False,) in match_dfs:
            i += 1
            match_dfs = (
                match_dfs[False,]
                .join(old_names_df.filter(pl.col("index") == i), on="path", how="left")
                .join(
                    new_names_df.filter(pl.col("index") == i), 
                    left_on=["path_right", "name"], 
                    right_on=["path", "name"],
                    how="left",
                )
                .with_columns(pl.col("index_right").is_null().all().over("path"))
                .partition_by("index_right", as_dict=True, include_key=False)
            )
            if (True,) in match_dfs:
                path_map_df.vstack(
                    match_dfs[True,]
                    .with_columns(pds.str_leven("path", "path_right", True, True).alias("similarity"))
                    .filter(pl.col("similarity").max().over("path").eq(pl.col("similarity")))
                    .group_by("path").first()
                    .select("path", "path_right"),
                    in_place=True
                )
            if (False,) in match_dfs:
                match_df = match_dfs[False,].select("path", "path_right")
            else:
                break
        else:
            break

    return path_map_df.rename({"path": "old_path", "path_right": "new_path"})

def map_path_by_match(old_src_df: pl.DataFrame, new_src_df: pl.DataFrame):
    return (
        old_src_df.filter(~pl.col("code").str.contains(r"^\s*#|^\s*[\{\}]$"))
        .with_columns(pl.col("line").rank("min").over(["path", "code"]).alias("rank"))
        .join(
            new_src_df.filter(~pl.col("code").str.contains(r"^\s*#|^\s*[\{\}]$"))
            .with_columns(pl.col("line").rank("min").over(["path", "code"]).alias("rank")), 
            on=["code", "rank"])
        .group_by("path", "path_right")
        .agg(pl.col("code").str.len_chars().sum().alias("len"))
        .filter(
            (pl.col("len") == pl.col("len").max().over("path")) &
            (pl.col("len") == pl.col("len").max().over("path_right"))
        )
        .filter(
            pl.col("path").is_unique() & pl.col("path_right").is_unique()
        )
        .rename({"path": "old_path", "path_right": "new_path"})
    )

def replace_with_path(df: pl.DataFrame, col: str, path_df: pl.DataFrame):
    return (
        df.join(
            map_path_by_name(
                df.select(pl.col(col).alias("path")).unique(), 
                path_df
            ),
            left_on=col, right_on="old_path", how="left",
        )
        .with_columns(pl.col("new_path").fill_null(pl.col(col)))
        .drop(col)
        .rename({"new_path": "path"})
    )