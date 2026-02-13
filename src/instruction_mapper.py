import polars as pl
from utils import levenshtein_similarity_expr

def process_ins(ins_df: pl.DataFrame):
    return  (
        ins_df
        .with_columns(
            pl.col("instruction")
            .str.splitn("\t", 2)
            .struct.rename_fields(["opcode", "operand"])
        )
        .unnest("instruction")
        .with_columns(
            pl.col("operand")
            .str.replace(r"0x[0-9a-f]+\s<", "<")
            .fill_null("")
        )
        .select(
            "fid", "path", "line", "bid", 
            pl.col("address").rank("min").alias("index"), "opcode", "operand",
        )
        .unique()
    )

def map_ins(old_ins_df: pl.DataFrame, new_ins_df: pl.DataFrame, fsrc_map_df: pl.DataFrame):
    old_ins_df = (
        process_ins(old_ins_df)
        .rename(lambda col: "old_" + col)
        .with_columns(pl.col("old_opcode").str.slice(0,1).alias("oid"))
    )
    new_ins_df = (
        process_ins(new_ins_df)
        .rename(lambda col: "new_" + col)
        .with_columns(pl.col("new_opcode").str.slice(0,1).alias("oid"))
    )
    
    ins_match_df = (
        old_ins_df
        .join(fsrc_map_df, on=["old_fid", "old_path", "old_line"])
        .join(new_ins_df, on=["new_fid", "new_path", "new_line", "oid"])
        .with_columns(
            levenshtein_similarity_expr("old_opcode", "new_opcode")
            .round(10)
            .alias("opcode_match_score")
        )
        .filter(
            pl.col("opcode_match_score").max()
            .over("old_path", "old_line", "new_path", "new_line", "old_index")
            .eq(pl.col("opcode_match_score")) |
            pl.col("opcode_match_score").max()
            .over("old_path", "old_line", "new_path", "new_line", "new_index")
            .eq(pl.col("opcode_match_score"))
        )
        .with_columns(
            pl.when(pl.col("oid") == "j")
            .then(
                pl.col("old_operand")
                .str.extract_groups(r"<(.+)\+0x([0-9a-f]+)>")
                .struct.rename_fields(["old_jump_name", "old_jump_offset"])
            )
            .alias("old_jump"),
            pl.when(pl.col("oid") == "j")
            .then(
                pl.col("new_operand")
                .str.extract_groups(r"<(.+)\+0x([0-9a-f]+)>")
                .struct.rename_fields(["new_jump_name", "new_jump_offset"])
            )
            .alias("new_jump"),
        )
        .unnest("old_jump", "new_jump")
        .with_columns(
            levenshtein_similarity_expr("old_jump_name", "new_jump_name")
            .round(10)
            .alias("jump_func_score"),
            (
                pl.col("old_jump_offset").str.to_integer(base=16) - 
                pl.col("new_jump_offset").str.to_integer(base=16)
            ).abs().neg()
            .alias("jump_diff_score"),
            levenshtein_similarity_expr("old_operand", "new_operand")
            .round(10)
            .alias("operand_match_score"),
        )
        .with_columns(pl.col("jump_func_score") + pl.col("opcode_match_score"))
    )

    ins_map_df = pl.DataFrame(
        schema=ins_match_df.select(
            "old_fid", "old_bid", "old_index", 
            "new_fid", "new_bid", "new_index",
            "jump_func_score", "opcode_match_score", 
            "jump_diff_score", "operand_match_score",
        ).schema
    )

    while True:
        temp_map_df = (
            ins_match_df.lazy()
            .filter(
                pl.col("jump_func_score").max()
                .over("old_fid", "old_bid", "old_index", "new_fid", "new_bid")
                .eq(pl.col("jump_func_score")) &
                pl.col("jump_func_score").max()
                .over("new_fid", "new_bid", "new_index", "old_fid", "old_bid")
                .eq(pl.col("jump_func_score"))
            )
            .filter(
                pl.col("opcode_match_score").max()
                .over("old_fid", "old_bid", "old_index", "new_fid", "new_bid")
                .eq(pl.col("opcode_match_score")) &
                pl.col("opcode_match_score").max()
                .over("new_fid", "new_bid", "new_index", "old_fid", "old_bid")
                .eq(pl.col("opcode_match_score"))
            )
            .filter(
                pl.col("jump_diff_score").max()
                .over("old_fid", "old_bid", "old_index", "new_fid", "new_bid")
                .eq_missing(pl.col("jump_diff_score")) &
                pl.col("jump_diff_score").max()
                .over("new_fid", "new_bid", "new_index", "old_fid", "old_bid")
                .eq_missing(pl.col("jump_diff_score"))
            )
            .filter(
                pl.col("operand_match_score").max()
                .over("old_fid", "old_bid", "old_index", "new_fid", "new_bid")
                .eq(pl.col("operand_match_score")) &
                pl.col("operand_match_score").max()
                .over("new_fid", "new_bid", "new_index", "old_fid", "old_bid")
                .eq(pl.col("operand_match_score"))
            )
            .filter(
                pl.col("new_index").rank()
                .over("old_fid", "old_bid", "old_index", "new_fid", "new_bid")
                ==
                pl.col("old_index").rank()
                .over("new_fid", "new_bid", "new_index", "old_fid", "old_bid")
            )
            .select(ins_map_df.columns)
            .collect()
        )

        if temp_map_df.is_empty():
            break

        ins_map_df.vstack(temp_map_df, in_place=True)
        
        ins_match_df = (
            ins_match_df
            .join(temp_map_df, on=["old_index", "old_fid", "old_bid"], how="anti")
            .join(temp_map_df, on=["new_index", "new_fid", "new_bid"], how="anti")
        )

    return ins_map_df
