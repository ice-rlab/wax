import polars as pl
import polars.selectors as cs
import polars_ds as pds
from path_mapper import map_path_by_name, map_path_by_match

def tight_bound_src(src_df: pl.DataFrame, s: str, o: str, src_map_df: pl.DataFrame):
    return (
        src_df.rename({"path": f"{s}_path", "line": f"{s}_line"})
        .join(
            src_map_df.select(
                f"{s}_path", f"{s}_line",
                pl.struct([f"{o}_path", f"{o}_line"])
                .struct.rename_fields([f"{o}_path_prev", f"{o}_line_prev"])
                .alias("loc_prev"),
                pl.struct([f"{o}_path", f"{o}_line"])
                .struct.rename_fields([f"{o}_path_next", f"{o}_line_next"])
                .alias("loc_next"),
                pl.col(f"{s}_path").alias("path_self"),
            ),
            on=[f"{s}_path", f"{s}_line"], 
            how="left",
        )
        .sort(f"{s}_path", f"{s}_line")
        .with_columns(
            pl.when(pl.col("path_self").fill_null(strategy="forward") == pl.col(f"{s}_path"))
            .then(pl.col("loc_prev").fill_null(strategy="forward"))
            .otherwise(pl.struct([
                pl.lit(None).alias(f"{o}_path_prev"), 
                pl.lit(0).alias(f"{o}_line_prev")
            ]))
            .struct.unnest(),
            pl.when(pl.col("path_self").fill_null(strategy="backward") == pl.col(f"{s}_path"))
            .then(pl.col("loc_next").fill_null(strategy="backward"))
            .otherwise(pl.struct([
                pl.lit(None).alias(f"{o}_path_next"), 
                pl.Int64.max().alias(f"{o}_line_next")
            ]))
            .struct.unnest(),
        )
        .drop("loc_prev", "loc_next")
        .with_columns(
            pl.col(f"{o}_path_prev")
            .fill_null(pl.col(f"{o}_path_next")),
            pl.col(f"{o}_path_next")
            .fill_null(pl.col(f"{o}_path_prev"))
        )
        .filter(
            pl.col("path_self").is_null() &
            pl.col(f"{o}_path_prev").eq(pl.col(f"{o}_path_next")) &
            pl.col(f"{o}_line_prev").lt(pl.col(f"{o}_line_next") - 1)
        )
        .drop(f"{o}_path_next", "path_self")
        .rename({f"{o}_path_prev": f"{o}_path"})
    )

def loose_bound_src(src_df: pl.DataFrame, s: str, o: str, src_map_df: pl.DataFrame):
    return pl.concat((
        src_df.rename({"path": f"{s}_path", "line": f"{s}_line"})
        .join(
            src_map_df.sort(f"{o}_path", f"{o}_line")
            .select(
                f"{s}_path", f"{s}_line",
                pl.struct(
                    f"{o}_path", 
                    pl.col(f"{o}_line").shift(shift_dir).fill_null(null_value)
                    .alias(f"{o}_line_{shift_name}"),
                    pl.col(f"{o}_line").alias(f"{o}_line_{self_name}"),
                ).alias("loc"),
                pl.col(f"{s}_path").alias("path_self"),
            ),
            on=[f"{s}_path", f"{s}_line"],
            how="left",
        )
        .sort(f"{s}_path", f"{s}_line")
        .with_columns(
            pl.when(pl.col("path_self").fill_null(strategy=strategy) == pl.col(f"{s}_path"))
            .then(pl.col("loc").fill_null(strategy=strategy))
            .otherwise(pl.struct(
                pl.lit(None).alias(f"{o}_path"), 
                pl.lit(None).alias(f"{o}_line_{shift_name}"), 
                pl.lit(None).alias(f"{o}_line_{self_name}")
            )).struct.unnest(),
        )
        .filter(pl.col("path_self").is_null())
        .drop("loc", "path_self")
        for shift_dir, shift_name, self_name, strategy, null_value in [
            (-1, "next", "prev", "forward",  pl.Int64.max()),
            ( 1, "prev", "next", "backward", pl.lit(0)),
        ]
    ), how="align")

def map_src_exact_unique(old_src_df: pl.DataFrame, new_src_df: pl.DataFrame, 
                         path_map_df: pl.DataFrame):
    src_map_df = (
        path_map_df
        .join(
            old_src_df.filter(pl.struct("path", "code").is_unique())
            .rename({"path": "old_path", "line": "old_line"}), 
            on="old_path",
        )
        .join(
            new_src_df.filter(pl.struct("path", "code").is_unique())
            .rename({"path": "new_path", "line": "new_line"}), 
            on=["new_path", "code"], 
        )
        .select("old_path", "old_line", "new_path", "new_line")
        .filter(
            pl.struct(["old_path", "old_line"]).is_unique() & 
            pl.struct(["new_path", "new_line"]).is_unique()
        )
    )

    removed_comments = False
    while True:
        temp_map_df = path_map_df.join(
            old_src_df
            .join(
                src_map_df.select(
                    pl.col("old_path").alias("path"), pl.col("old_line").alias("line"),
                    pl.col("new_line").alias("line_prev"), pl.col("new_line").alias("line_next"),
                    pl.col("old_path").alias("path_self"),
                ),
                on=["path", "line"], 
                how="left",
            )
            .sort("path", "line")
            .with_columns(
                pl.when(pl.col("path_self").fill_null(strategy="forward") == pl.col("path"))
                .then(pl.col("line_prev").fill_null(strategy="forward")).otherwise(0),
                pl.when(pl.col("path_self").fill_null(strategy="backward") == pl.col("path"))
                .then(pl.col("line_next").fill_null(strategy="backward")).otherwise(pl.Int64.max()),
            )
            .filter(pl.col("path_self").is_null())
            .drop("path_self")
            .filter(pl.struct("path", "code").is_unique())
            .rename(lambda col: ("old_" + col) if col != "code" else col), 
            on="old_path",
        ).join(
            new_src_df
            .join(
                src_map_df.select(
                    pl.col("new_path").alias("path"), pl.col("new_line").alias("line"),
                    pl.col("old_line").alias("line_prev"), pl.col("old_line").alias("line_next"),
                    pl.col("new_path").alias("path_self"),
                ),
                on=["path", "line"], 
                how="left",
            )
            .sort("path", "line")
            .with_columns(
                pl.when(pl.col("path_self").fill_null(strategy="forward") == pl.col("path"))
                .then(pl.col("line_prev").fill_null(strategy="forward")).otherwise(0),
                pl.when(pl.col("path_self").fill_null(strategy="backward") == pl.col("path"))
                .then(pl.col("line_next").fill_null(strategy="backward")).otherwise(pl.Int64.max()),
            )
            .filter(pl.col("path_self").is_null())
            .drop("path_self")
            .filter(pl.struct("path", "code").is_unique())
            .rename(lambda col: ("new_" + col) if col != "code" else col),
            on=["new_path", "code"], 
        ).filter(
            pl.col("old_line").is_between("new_line_prev", "new_line_next", closed="none") &
            pl.col("new_line").is_between("old_line_prev", "old_line_next", closed="none")
        ).select(
            "old_path", "old_line", "new_path", "new_line"
        ).filter(
            pl.struct(["old_path", "old_line"]).is_unique() & 
            pl.struct(["new_path", "new_line"]).is_unique()
        )

        if temp_map_df.is_empty():
            if removed_comments:
                break
            old_src_df = old_src_df.with_columns(pl.col("code").str.replace(r"//.*$", ""))
            new_src_df = new_src_df.with_columns(pl.col("code").str.replace(r"//.*$", ""))
            removed_comments = True

        src_map_df.vstack(temp_map_df, in_place=True)
    
    return src_map_df

def bound_rank_src(src_map_df: pl.DataFrame, src_df: pl.DataFrame, s: str, o: str):
    return (
        src_df.rename({"path": f"{s}_path", "line": f"{s}_line"})
        .join(
            src_map_df.select(
                f"{s}_path", f"{s}_line",
                pl.struct([f"{o}_path", f"{o}_line"])
                .struct.rename_fields([f"{o}_path_prev", f"{o}_line_prev"])
                .alias("loc_prev"),
                pl.struct([f"{o}_path", f"{o}_line"])
                .struct.rename_fields([f"{o}_path_next", f"{o}_line_next"])
                .alias("loc_next"),
                pl.col(f"{s}_path").alias("path_self"),
            ),
            on=[f"{s}_path", f"{s}_line"], 
            how="left",
        )
        .sort(f"{s}_path", f"{s}_line")
        .with_columns(
            pl.when(pl.col("path_self").fill_null(strategy="forward") == pl.col(f"{s}_path"))
            .then(pl.col("loc_prev").fill_null(strategy="forward"))
            .otherwise(pl.struct([pl.lit(None).alias(f"{o}_path_prev"), pl.lit(0).alias(f"{o}_line_prev")]))
            .struct.unnest(),
            pl.when(pl.col("path_self").fill_null(strategy="backward") == pl.col(f"{s}_path"))
            .then(pl.col("loc_next").fill_null(strategy="backward"))
            .otherwise(pl.struct([pl.lit(None).alias(f"{o}_path_next"), pl.Int64.max().alias(f"{o}_line_next")]))
            .struct.unnest(),
        )
        .with_columns(
            pl.col(f"{o}_path_prev")
            .fill_null(pl.col(f"{o}_path_next")),
            pl.col(f"{o}_path_next")
            .fill_null(pl.col(f"{o}_path_prev"))
        )
        .filter(
            pl.col("path_self").is_null() &
            pl.col(f"{o}_path_prev").eq(pl.col(f"{o}_path_next")) &
            pl.col(f"{o}_line_prev").lt(pl.col(f"{o}_line_next"))
        )
        .select(
            f"{s}_path", f"{s}_line", "code",
            pl.col(f"{o}_path_prev").alias(f"{o}_path"),
            f"{o}_line_prev", f"{o}_line_next",
        )
        .with_columns(
            pl.col(f"{s}_line").rank("min", descending=False).over([f"{s}_path", "code", f"{o}_line_prev"]).alias("rank_inc"),
            pl.col(f"{s}_line").rank("min", descending=True).over([f"{s}_path", "code", f"{o}_line_prev"]).alias("rank_dec"),
            pl.col(f"{s}_line").count().over([f"{s}_path", "code", f"{o}_line_prev"]).alias("rank_count"),
        )
    )

def bound_unique(match_df: pl.DataFrame):
    return (
        match_df
        .filter(
            pl.col("old_line").is_between(pl.col("old_line_prev"), pl.col("old_line_next"), closed="none") &
            pl.col("new_line").is_between(pl.col("new_line_prev"), pl.col("new_line_next"), closed="none")
        )
        .with_columns(
            pl.min_horizontal(
                pl.col("old_line") - pl.col("old_line_prev"),
                pl.col("old_line_next") - pl.col("old_line")
            ).alias("old_dist"),
            pl.min_horizontal(
                pl.col("new_line") - pl.col("new_line_prev"),
                pl.col("new_line_next") - pl.col("new_line"),
            ).alias("new_dist")
        )
        .filter(
            (pl.col("old_dist") == pl.col("old_dist").min().over("old_path", "old_line")) &
            (pl.col("new_dist") == pl.col("new_dist").min().over("new_path", "new_line"))
        )
        .select("old_path", "old_line", "new_path", "new_line")
        .filter(
            pl.struct(["old_path", "old_line"]).is_unique() & 
            pl.struct(["new_path", "new_line"]).is_unique()
        )
    )

def map_src_exact_duplicate_bound(old_src_df: pl.DataFrame, new_src_df: pl.DataFrame, src_map_df: pl.DataFrame):
    old_bnd_df = bound_rank_src(src_map_df, old_src_df, "old", "new")
    new_bnd_df = bound_rank_src(src_map_df, new_src_df, "new", "old")

    removed_comments = False
    while True:
        src_map_df = bound_unique(
            old_bnd_df.join(new_bnd_df, on=["old_path", "new_path", "code", "rank_count", "rank_inc"])
        )

        if not src_map_df.is_empty():
            return src_map_df

        src_map_df = pl.concat([
            bound_unique(
                old_bnd_df.filter(pl.col("rank_inc") == 1)
                .join(
                    new_bnd_df.filter(pl.col("rank_inc") == 1), 
                    on=["old_path", "new_path", "code", "rank_inc"],
                )
            ),
            bound_unique(
                old_bnd_df.filter((pl.col("rank_inc") != 1) & (pl.col("rank_dec") == 1))
                .join(
                    new_bnd_df.filter((pl.col("rank_inc") != 1) & (pl.col("rank_dec") == 1)), 
                    on=["old_path", "new_path", "code", "rank_dec"],
                )
            )
        ])

        if not src_map_df.is_empty():
            return src_map_df
        
        if removed_comments:
            break
        
        old_bnd_df = old_bnd_df.with_columns(pl.col("code").str.replace(r"//.*$", ""))
        new_bnd_df = new_bnd_df.with_columns(pl.col("code").str.replace(r"//.*$", ""))
        removed_comments = True

    return src_map_df

def map_src_exact_word_bound(old_src_df: pl.DataFrame, new_src_df: pl.DataFrame, src_map_df: pl.DataFrame):
    old_bnd_df = tight_bound_src(old_src_df, "old", "new", src_map_df)
    new_bnd_df = tight_bound_src(new_src_df, "new", "old", src_map_df)

    map_df = src_map_df.sample(0)

    match_df = (
        old_bnd_df.lazy()
        .with_columns(pl.col("code").str.replace(r"//.*$", ""))
        .with_columns(
            pl.col("code").str.strip_chars()
            .str.replace_all(r"\s+", " ")
            .str.split(" ").alias("word"))
        .with_columns(pl.int_ranges(0, pl.col("word").list.len()).alias("index"))
        .explode("word", "index")
        .with_columns(pl.col("index").rank("min").over("old_path", "old_line", "word"))
    ).join(
        new_bnd_df.lazy()
        .with_columns(pl.col("code").str.replace(r"//.*$", ""))
        .with_columns(
            pl.col("code").str.strip_chars()
            .str.replace_all(r"\s+", " ")
            .str.split(" ").alias("word"))
        .with_columns(pl.int_ranges(0, pl.col("word").list.len()).alias("index"))
        .explode("word", "index")
        .with_columns(pl.col("index").rank("min").over("new_path", "new_line", "word")),
        on=["old_path", "new_path", "word", "index"]
    ).filter(
        pl.col("old_line").is_between("old_line_prev", "old_line_next", closed="none") &
        pl.col("new_line").is_between("new_line_prev", "new_line_next", closed="none")
    ).group_by(
        "old_path", "old_line", "new_path", "new_line", "code", "code_right"
    ).agg(
        (pl.col("word").str.len_chars() / 
        pl.max_horizontal(pl.col("code").str.len_chars(), pl.col("code_right").str.len_chars())
        ).sum().alias("len")
    ).collect()
    
    while True:
        temp_map_df = match_df.filter(
            pl.col("len").max().over("old_path", "old_line").eq(pl.col("len")) &
            pl.col("len").max().over("new_path", "new_line").eq(pl.col("len"))
        ).filter(
            pl.struct("old_path", "old_line").is_unique() &
            pl.struct("new_path", "new_line").is_unique()
        ).select(map_df.columns)

        if temp_map_df.is_empty():
            break
        map_df.vstack(temp_map_df, in_place=True)
        match_df = (
            match_df.join(temp_map_df, on=["old_path", "old_line"], how="anti")
            .join(temp_map_df, on=["new_path", "new_line"], how="anti")
        )
    
    match_df = match_df.with_columns(
        pds.str_leven("code", "code_right", True, True).alias("similarity")
    )
    
    while True:
        temp_map_df = match_df.filter(
            pl.col("len").max().over("old_path", "old_line").eq(pl.col("len")) &
            pl.col("len").max().over("new_path", "new_line").eq(pl.col("len"))
        ).filter(
            pl.col("similarity").max().over("old_path", "old_line").eq(pl.col("similarity")) &
            pl.col("similarity").max().over("new_path", "new_line").eq(pl.col("similarity"))
        ).filter(
            pl.struct("old_path", "old_line").is_unique() &
            pl.struct("new_path", "new_line").is_unique()
        ).select(map_df.columns)

        if temp_map_df.is_empty():
            break
        map_df.vstack(temp_map_df, in_place=True)
        match_df = (
            match_df.join(temp_map_df, on=["old_path", "old_line"], how="anti")
            .join(temp_map_df, on=["new_path", "new_line"], how="anti")
        )
    
    return map_df

def map_src_fuzzy_tight_bound(old_src_df: pl.DataFrame, new_src_df: pl.DataFrame, 
                              old_line_df: pl.DataFrame, new_line_df: pl.DataFrame, 
                              src_map_df: pl.DataFrame):
    old_bnd_df = (
        tight_bound_src(old_src_df, "old", "new", src_map_df)
    )
    old_bnd_df = old_bnd_df.join(old_line_df, left_on=["old_path", "old_line"], right_on=["path", "line"])
    new_bnd_df = (
        tight_bound_src(new_src_df, "new", "old", src_map_df)
    )
    new_bnd_df = new_bnd_df.join(new_line_df, left_on=["new_path", "new_line"], right_on=["path", "line"])
    
    return (
        old_bnd_df.lazy().join(new_bnd_df.lazy(), on=["old_path", "new_path"])
        .filter(
            pl.col("old_line").is_between("old_line_prev", "old_line_next", closed="none") &
            pl.col("new_line").is_between("new_line_prev", "new_line_next", closed="none")
        )
        .with_columns(
            pds.str_leven("code", "code_right", True, True).alias("similarity")
        )
        .filter(
            pl.col("similarity").max().over("old_path", "old_line")
            .eq(pl.col("similarity")) &
            pl.col("similarity").max().over("new_path", "new_line")
            .eq(pl.col("similarity"))
        )
        .with_columns(
            pl.max_horizontal(
                pl.min_horizontal(
                    pl.col("old_line") - pl.col("old_line_prev"),
                    pl.col("old_line_next") - pl.col("old_line")
                ),
                pl.min_horizontal(
                    pl.col("new_line") - pl.col("new_line_prev"),
                    pl.col("new_line_next") - pl.col("new_line"),
                )
            ).alias("diff")
        )
        .filter(
            pl.col("diff").min().over("old_path", "old_line")
            .eq(pl.col("diff")) &
            pl.col("diff").min().over("new_path", "new_line")
            .eq(pl.col("diff"))
        )
        .filter(
            pl.struct(["old_path", "old_line"]).is_unique() & 
            pl.struct(["new_path", "new_line"]).is_unique()
        )
        .select("old_path", "old_line", "new_path", "new_line")
        .collect()
    )

def map_src(old_src_df: pl.DataFrame, new_src_df: pl.DataFrame,
            old_line_df: pl.DataFrame, new_line_df: pl.DataFrame):
    path_map_df = map_path_by_name(
        old_src_df.select("path").unique(), 
        new_src_df.select("path").unique(),
    )

    t_old_src_df = old_src_df
    t_new_src_df = new_src_df

    src_map_df = pl.DataFrame(
        schema={"old_path": str, "old_line": int, "new_path": str, "new_line": int}
    )
    path_used = False

    while True:
        t_old_src_df = t_old_src_df.join(
            src_map_df, 
            left_on=["path", "line"], 
            right_on=["old_path", "old_line"], 
            how="anti",
        )
        t_new_src_df = t_new_src_df.join(
            src_map_df, 
            left_on=["path", "line"], 
            right_on=["new_path", "new_line"], 
            how="anti",
        )

        new_data = False
        temp_map_df = map_src_exact_unique(t_old_src_df, t_new_src_df, path_map_df)
        if not temp_map_df.is_empty():
            src_map_df.vstack(temp_map_df, in_place=True)
            path_used = True
            new_data = True

        temp_map_df = map_src_exact_duplicate_bound(old_src_df, new_src_df, src_map_df)
        if not temp_map_df.is_empty():
            src_map_df.vstack(temp_map_df, in_place=True)
            path_used = True
            new_data = True
        
        if new_data:
            continue

        if not path_used:
            break

        path_map_df = map_path_by_match(t_old_src_df, t_new_src_df)
        path_used = False

    t_old_src_df = t_old_src_df.join(old_line_df, on=["path", "line"])
    t_new_src_df = t_new_src_df.join(new_line_df, on=["path", "line"])

    while True:
        t_old_src_df = t_old_src_df.join(
            src_map_df, 
            left_on=["path", "line"], 
            right_on=["old_path", "old_line"], 
            how="anti",
        )
        t_new_src_df = t_new_src_df.join(
            src_map_df, 
            left_on=["path", "line"], 
            right_on=["new_path", "new_line"], 
            how="anti",
        )

        new_data = False
        temp_map_df = map_src_exact_unique(t_old_src_df, t_new_src_df, path_map_df)
        if not temp_map_df.is_empty():
            src_map_df.vstack(temp_map_df, in_place=True)
            path_used = True
            new_data = True

        temp_map_df = map_src_exact_duplicate_bound(old_src_df, new_src_df, src_map_df)
        if not temp_map_df.is_empty():
            src_map_df.vstack(temp_map_df, in_place=True)
            path_used = True
            new_data = True
        
        if new_data:
            continue

        temp_map_df = map_src_fuzzy_tight_bound(old_src_df, new_src_df, old_line_df, new_line_df, src_map_df)
        if not temp_map_df.is_empty():
            src_map_df.vstack(temp_map_df, in_place=True)
            path_used = True
            new_data = True
        
        if new_data:
            continue

        break
    
    return src_map_df

def map_src_with_func(old_fsrc_df: pl.DataFrame, new_fsrc_df: pl.DataFrame, 
                       func_map_df: pl.DataFrame, src_map_df: pl.DataFrame,
                       map_remaining: bool):
    temp_map_df = src_map_df.sample(0)
    while True:
        src_map_df.vstack(temp_map_df, in_place=True)

        old_bnd_df = tight_bound_src(
            old_fsrc_df.select("path", "line", "code").unique(), 
            "old", "new",
            src_map_df, 
        )
        new_bnd_df = tight_bound_src(
            new_fsrc_df.select("path", "line", "code").unique(),
            "new", "old",
            src_map_df, 
        )
        temp_map_df = (
            old_bnd_df.join(new_bnd_df, on=["old_path", "new_path", "code"])
            .filter(
                pl.col("old_line").is_between("old_line_prev", "old_line_next", closed="none") &
                pl.col("new_line").is_between("new_line_prev", "new_line_next", closed="none")
            )
            .filter(
                pl.struct("old_path", "old_line").is_unique() &
                pl.struct("new_path", "new_line").is_unique()
            )
            .select(src_map_df.columns)
        )
        if not temp_map_df.is_empty():
            continue

        old_bnd_df = loose_bound_src(
            old_fsrc_df.select("path", "line", "code").unique(), 
            "old", "new",
            src_map_df, 
        )
        new_bnd_df = loose_bound_src(
            new_fsrc_df.select("path", "line", "code").unique(),
            "new", "old",
            src_map_df, 
        )
        temp_map_df = (
            old_bnd_df.join(new_bnd_df, on=["old_path", "new_path", "code"])
            .filter(
                pl.col("old_line").is_between("old_line_prev", "old_line_next", closed="none") &
                pl.col("new_line").is_between("new_line_prev", "new_line_next", closed="none")
            )
            .select(src_map_df.columns)
            .unique()
            .filter(
                pl.struct("old_path", "old_line").is_unique() &
                pl.struct("new_path", "new_line").is_unique()
            )
        )
        if not temp_map_df.is_empty():
            continue

        temp_map_df = (
            old_bnd_df.join(new_bnd_df, on=["old_path", "new_path", "code"])
            .select(src_map_df.columns)
            .unique()
            .filter(
                pl.struct("old_path", "old_line").is_unique() &
                pl.struct("new_path", "new_line").is_unique()
            )
        )
        if not temp_map_df.is_empty():
            continue

        old_fbnd_df = (
            old_bnd_df.join(
                old_fsrc_df.select(cs.by_name("fid", "path", "line").name.prefix("old_")), 
                on=["old_path", "old_line"],
            )
            .with_columns(pl.n_unique("old_fid").over("old_path", "old_line").alias("count"))
        )
        new_fbnd_df = (
            new_bnd_df.join(
                new_fsrc_df.select(cs.by_name("fid", "path", "line").name.prefix("new_")), 
                on=["new_path", "new_line"],
            )
            .with_columns(pl.n_unique("new_fid").over("new_path", "new_line").alias("count"))
        )
        temp_map_df = (
            old_fbnd_df.join(func_map_df, on="old_fid")
            .join(new_fbnd_df, on=["new_fid", "old_path", "new_path", "code"])
            .filter(pl.col("count") == pl.col("count_right"))
            .select(
                "old_fid", "old_path", "old_line", 
                "new_fid", "new_path", "new_line", "count",
            )
            .unique()
            .group_by("old_path", "old_line", "new_path", "new_line", "count").len()
            .filter(pl.col("count") == pl.col("len"))
            .filter(
                pl.struct("old_path", "old_line").is_unique() &
                pl.struct("new_path", "new_line").is_unique()
            )
            .select(src_map_df.columns)
        )
        if not temp_map_df.is_empty():
            continue

        break

    fsrc_map_df = (
        src_map_df.join(
            old_fsrc_df.select(cs.by_name("fid", "path", "line").name.prefix("old_")), 
            on=["old_path", "old_line"],
        )
        .join(func_map_df, on="old_fid")
        .join(
            new_fsrc_df.select(cs.by_name("fid", "path", "line").name.prefix("new_")), 
            on=["new_fid", "new_path", "new_line"],
        )
        .select("old_fid", "old_path", "old_line", "new_fid", "new_path", "new_line")
    )

    if not map_remaining:
        return fsrc_map_df
    
    fsrc_dfs = [old_fsrc_df, new_fsrc_df]
    vers = ["old", "new"]

    temp_map_df = (
        old_fbnd_df.join(func_map_df, on="old_fid")
        .join(new_fbnd_df, on=["new_fid", "old_path", "new_path"])
        .select(
            "old_fid", "old_path", "old_line", 
            "new_fid", "new_path", "new_line", 
            "code", "code_right",
        )
        .unique()
        .with_columns(
            pds.str_leven("code", "code_right", True, True).alias("similarity")
        )
        .filter(pl.col("similarity") >= 0.9)
        .filter(
            pl.col("similarity").max().over("old_path", "old_line")
            .eq(pl.col("similarity")) |
            pl.col("similarity").max().over("new_path", "new_line")
            .eq(pl.col("similarity"))
        )
        .sort("similarity")
        .select(fsrc_map_df.columns)
    )
    fsrc_map_df.vstack(temp_map_df, in_place=True)

    temp_map_df = (
        (
            old_fsrc_df.select(pl.all().name.prefix("old_"))
            .join(fsrc_map_df, on=["old_fid", "old_path", "old_line"], how="anti")
        )
        .join(func_map_df, on="old_fid")
        .join(
            fsrc_map_df.select("old_path", "new_path", "new_fid").unique(),
            on=["old_path", "new_fid"],
        )
        .join(
            new_fsrc_df.select(pl.all().name.prefix("new_"))
            .join(fsrc_map_df, on=["new_fid", "new_path", "new_line"], how="anti"),
            on=["new_fid", "new_path"],
        )
        .with_columns(
            pds.str_leven("old_code", "new_code", True, True).alias("similarity")
        ).filter(
            pl.col("similarity").max().over("old_fid", "old_path", "old_line")
            .eq(pl.col("similarity")) |
            pl.col("similarity").max().over("new_fid", "new_path", "new_line")
            .eq(pl.col("similarity"))
        )
        .select(fsrc_map_df.columns)
        .unique()
    )
    fsrc_map_df.vstack(temp_map_df, in_place=True)

    temp_map_df = pl.concat(
        fsrc_dfs[i].select(pl.all().name.prefix(f"{vers[i]}_"))
        .join(fsrc_map_df, on=[f"{vers[i]}_fid", f"{vers[i]}_path", f"{vers[i]}_line"], how="anti")
        .join(func_map_df, on=f"{vers[i]}_fid")
        .join(
            fsrc_map_df.select(f"{vers[i]}_path", f"{vers[1-i]}_fid", f"{vers[1-i]}_path").unique(),
            on=[f"{vers[i]}_path", f"{vers[1-i]}_fid"],
        )
        .join(
            fsrc_dfs[1-i].select(pl.all().name.prefix(f"{vers[1-i]}_")),
            on=[f"{vers[1-i]}_fid", f"{vers[1-i]}_path"],
        ).with_columns(
            pds.str_leven(f"{vers[i]}_code", f"{vers[1-i]}_code", True, True).alias("similarity")
        ).filter(
            pl.col("similarity").max().over(f"{vers[i]}_fid", f"{vers[i]}_path", f"{vers[i]}_line")
            .eq(pl.col("similarity"))
        )
        .select(fsrc_map_df.columns)
        .unique()
        for i in range(2)
    )
    fsrc_map_df.vstack(temp_map_df, in_place=True)

    temp_map_df = (
        (
            old_fsrc_df.select(pl.all().name.prefix("old_"))
            .join(fsrc_map_df, on=["old_fid", "old_path", "old_line"], how="anti")
        )
        .join(func_map_df, on="old_fid")
        .join(
            new_fsrc_df.select(pl.all().name.prefix("new_"))
            .join(fsrc_map_df, on=["new_fid", "new_path", "new_line"], how="anti"),
            on="new_fid",
        ).with_columns(
            pds.str_leven("old_code", "new_code", True, True).alias("similarity")
        ).filter(
            pl.col("similarity").max().over("old_fid", "old_path", "old_line")
            .eq(pl.col("similarity")) |
            pl.col("similarity").max().over("new_fid", "new_path", "new_line")
            .eq(pl.col("similarity"))
        )
        .select(fsrc_map_df.columns)
        .unique()
    )
    fsrc_map_df.vstack(temp_map_df, in_place=True)

    temp_map_df = pl.concat(
        fsrc_dfs[i].select(pl.all().name.prefix(f"{vers[i]}_"))
        .join(fsrc_map_df, on=[f"{vers[i]}_fid", f"{vers[i]}_path", f"{vers[i]}_line"], how="anti")
        .join(func_map_df, on=f"{vers[i]}_fid")
        .join(
            fsrc_dfs[1-i].select(pl.all().name.prefix(f"{vers[1-i]}_")),
            on=[f"{vers[1-i]}_fid"],
        ).with_columns(
            pds.str_leven(f"{vers[i]}_code", f"{vers[1-i]}_code", True, True).alias("similarity")
        ).filter(
            pl.col("similarity").max().over(f"{vers[i]}_fid", f"{vers[i]}_path", f"{vers[i]}_line")
            .eq(pl.col("similarity"))
        )
        .select(fsrc_map_df.columns)
        .unique()
        for i in range(2)
    )
    fsrc_map_df.vstack(temp_map_df, in_place=True)

    return fsrc_map_df
