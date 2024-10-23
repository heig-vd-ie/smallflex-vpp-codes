import re
import uuid
import json
from datetime import timedelta, datetime
from math import prod
from typing import Optional

import numpy as np
import polars as pl
from polars import col as c

from utility.general_function import modify_string, generate_uuid
# Global variable


def modify_string_col(string_col: pl.Expr, format_str: dict) -> pl.Expr:
    """
    Modify string columns based on a given format dictionary.

    Args:
        string_col (pl.Expr): The string column to modify.
        format_str (dict): The format dictionary containing the string modifications.

    Returns:
        pl.Expr: The modified string column.
    """
    return string_col.map_elements(lambda x: modify_string(string=x, format_str=format_str), return_dtype=pl.Utf8, skip_nulls=True)

def cast_boolean(col: pl.Expr) -> pl.Expr:
    """
    Cast a column to boolean based on predefined replacements.

    Args:
        col (pl.Expr): The column to cast.

    Returns:
        pl.Expr: The casted boolean column.
    """
    replace = {
        "1": True, "true": True , "oui": True, "0": False, "false": False, "non": False}
    return col.str.to_lowercase().replace(replace, default=False).cast(pl.Boolean)


def generate_random_uuid(col: pl.Expr) -> pl.Expr:
    """
    Generate a random UUID.

    Returns:
        str: The generated UUID.
    """
    return col.map_elements(lambda x: str(uuid.uuid4()), return_dtype=pl.Utf8, skip_nulls=False)

def generate_uuid_col(col: pl.Expr, base_uuid: uuid.UUID  | None = None, added_string: str = "") -> pl.Expr:
    """
    Generate UUIDs for a column based on a base UUID and an optional added string.

    Args:
        col (pl.Expr): The column to generate UUIDs for.
        base_uuid (str): The base UUID for generating the UUIDs.
        added_string (str, optional): The optional added string. Defaults to "".

    Returns:
        pl.Expr: The column with generated UUIDs.
    """

    return (
        col.cast(pl.Utf8)
        .map_elements(lambda x: generate_uuid(base_value=x, base_uuid=base_uuid, added_string=added_string), pl.Utf8)
    )
