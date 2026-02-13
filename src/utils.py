import os
import subprocess
import polars as pl
import polars_ds as pds

def levenshtein_similarity_expr(c1: str | pl.Expr, c2: str | pl.Expr):
    return pds.str_leven(c1, c2, True, True)

def read_yaml_func(yaml_path):
    return (
        pl.read_csv(yaml_path, separator="?", has_header=False)
        .with_row_index()
        .filter(pl.col("column_1").str.starts_with("  - name:"))
        .select(
            "index", 
            pl.col("column_1")
            .str.replace(r"^  - name:\s+", "")
            .str.replace_all("'", "")
            .alias("func")
        )
    )

def read_paths(src_paths):
    return (
        pl.from_dict({
            "column_1": subprocess.Popen(
                rf'find -L {" ".join(src_paths)} -type f', 
                text=True, 
                shell=True, 
                stdout=subprocess.PIPE,
            ).stdout
        })
        .select(pl.col("column_1").str.replace("\n", "").alias("path"))
    )

def read_func_info(log_path: str):
    df = (
        pl.read_csv(log_path, separator="?", has_header=False)
        .filter(pl.col("column_1").str.starts_with("Tawhid-LineInfo"))
        .with_columns(
            pl.col("column_1")
            .str.splitn("#", 12)
            .struct.rename_fields(
                ["type", "func", "namespace", "basename", "parameters", "extension", 
                 "file", "line", "column", "fid", "hash", "nblocks"]
            )
        )
        .unnest("column_1")
        .drop("type")
        .with_columns(
            pl.col("func").str.extract(r"^[^/]*/(.*)$").fill_null("1").alias("chunk"), 
            pl.col("line").cast(int), 
            pl.col("column").cast(int),
        )
    )

    return (
        df.join(
            df.select("file").unique()
            .with_columns(
                pl.col("file")
                .map_elements(lambda f: os.path.normpath(f), return_dtype=str)
                .alias("file_norm")
            ),
            on="file",
        )
        .drop("file")
        .rename({"file_norm": "file"})
        .with_columns(
            pl.col("line").rank("dense").over("file").alias("rank")
        )
    )

def read_cfgs(file):
    df = (
        pl.read_csv(file, separator="?", has_header=False)
        .filter(pl.col("column_1").str.starts_with("Tawhid-Counts:"))
        .with_columns(
            pl.col("column_1")
            .str.splitn(" ", 3)
            .struct.rename_fields(["command", "type", "value"])
        )
        .unnest("column_1")
        .drop("command")
    )

    func_df = (
        df.filter(pl.col("type") == "func")
        .select(
            pl.col("value")
            .str.splitn(" ", 5)
            .struct.rename_fields(["func", "count", "fid", "has_profile", "is_ignored"])
        )
        .unnest("value")
        .select(
            pl.col("fid").cast(pl.UInt64), 
            pl.col("func").str.strip_chars_end(",").str.split(","), 
            pl.col("count").cast(pl.UInt64).reinterpret(signed=True),
        )
        .explode("func")
    )

    node_df = (
        df.filter(pl.col("type") == "node")
        .select(
            pl.col("value")
            .str.splitn(" ", 6)
            .struct.rename_fields(["block", "count", "start_address", "end_address", "hash", "insns"])
        )
        .unnest("value")
        .select(
            pl.col("block").str.splitn("#", 2)
            .struct.rename_fields(["fid", "bid"])
            .struct.with_fields(pl.field(["fid", "bid"]).cast(pl.UInt64))
            .struct.unnest(),
            "start_address", "end_address",
            "hash", "insns", "count",
        )
        .select(
            "fid", "bid",
            pl.col("start_address").cast(pl.UInt64) + pl.col("fid"),
            pl.col("end_address").cast(pl.UInt64) + pl.col("fid"),
            pl.col("hash").cast(pl.UInt64), pl.col("insns").cast(pl.UInt64), 
            pl.col("count").cast(pl.UInt64).reinterpret(signed=True),
        )
    )

    edge_df = (
    df.filter(pl.col("type") == "edge")
    .select(
        pl.col("value")
        .str.splitn(" ", 3)
        .struct.rename_fields(["src_block", "dst_block", "count"])
    )
    .unnest("value")
    .select(
        pl.col("src_block").str.splitn("#", 2)
        .struct.rename_fields(["src_fid", "src_bid"])
        .struct.with_fields(pl.field(["src_fid", "src_bid"]).cast(pl.UInt64))
        .struct.unnest(),
        pl.col("dst_block").str.splitn("#", 2)
        .struct.rename_fields(["dst_fid", "dst_bid"])
        .struct.with_fields(pl.field(["dst_fid", "dst_bid"]).cast(pl.UInt64))
        .struct.unnest(),
    )
)

    return func_df, node_df, edge_df

def read_srcs(paths):
    src_dfs: list[pl.DataFrame] = []

    for path in paths:
        with open(path) as f:
            src_dfs.append(
                pl.from_dict({"code": f.readlines()})
                .with_columns(pl.lit(path).alias("path"))
                .with_row_index(name="line", offset=1)
            )
    return (
        pl.concat(src_dfs)
        .with_columns(
            pl.format("{} //{}",
                pl.col("code").str.strip_chars_end(), "line"
            )
        )
        .group_by("path")
        .agg(pl.col("code").str.join("\n"))
        .with_columns(
            pl.col("code").str.replace_all(r"(?s)/\*.*?\*/", "")
            .str.split("\n")
        )
        .explode("code")
        .filter(~pl.col("code").str.contains(r"^[ \t]+//\d+$"))
        .with_columns(
            pl.col("code").str.extract_groups(r"^(?<code>.*) //(?<line>\d+)$")
            .struct.unnest()
        )
        .with_columns(pl.col("line").cast(int))
    )

def read_ins(debug_path: str, node_df: pl.LazyFrame):
    debug_df = (
        pl.scan_csv(debug_path, separator="?", has_header=False)
        .with_row_index()
    )

    return (
        (
            debug_df.filter(pl.col("column_1").str.starts_with(" "))
            .with_columns(
                pl.col("column_1")
                .str.strip_chars()
                .str.splitn(":", 2)
                .struct.rename_fields(["address", "instruction"])
            )
            .unnest("column_1")
            .with_columns(pl.col("instruction").str.strip_chars())
            .filter(pl.col("instruction").is_not_null())
        )
        .join_asof(
            debug_df.filter(pl.col("column_1").str.starts_with(";"))
            .with_columns(
                pl.col("column_1")
                .str.replace(r"^;\s", "")
                .str.replace_all(r"/(\./)+", "/")
                .str.replace(r"^\./", "")
                .str.replace(r"obj/", "", literal=True)
                .str.replace(r".*/\.\./", "")
                .str.splitn(":", 2)
                .struct.rename_fields(["file", "line"])
            )
            .unnest("column_1")
            .with_columns(pl.col("line").cast(int, strict=False))
            .filter(pl.col("line").is_not_null()),
            on="index",
        )
        .with_columns(pl.col("address").str.to_integer(base=16))
        .join_asof(
            node_df.sort("start_address"), 
            left_on="address", 
            right_on="start_address",
        )
        .filter(
            pl.col("address").is_between(
                pl.col("start_address"), 
                pl.col("end_address"), 
                closed="left",
            ) &
            pl.col("file").is_not_null() &
            pl.col("line").is_not_null()
        )
        .select("fid", "bid", "file", "line", "address", "instruction")
    )
