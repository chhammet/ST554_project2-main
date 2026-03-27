

#### Python file for data checking in Spark. 
#### Name: Cole Hammett
#### Date: 27th of March, 2026
#### Purpose: This file contains functions for data checks on Spark DataFrames (null values, duplicates, and data types). 

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd

class SparkDataCheck:

    def __init__(self, df):
        """Store the Spark SQL DataFrame as an instance attribute."""
        self.df = df


    @classmethod
    def from_csv(cls, spark, path):
        """
        Create a SparkDataCheck instance by reading a CSV file.

        Parameters
        ----------
        spark : SparkSession
        path : str

        Returns
        -------
        SparkDataCheck
        """
        df = spark.read.load(
            path,
            format="csv",
            inferSchema="true",
            header="true"
        )
        return cls(df)

    @classmethod
    def from_pandas(cls, spark, pandas_df):
        """
        Create a SparkDataCheck instance from a standard pandas DataFrame.

        Parameters
        ----------
        spark : SparkSession
        pandas_df : pandas.DataFrame

        Returns
        ----------
        SparkDataCheck
        """
        df = spark.createDataFrame(pandas_df)
        return cls(df)
    
    def check_numeric_range(self, col_name, lower=None, upper=None):
        """
        Appends a boolean column (<col_name>_in_range) indicating whether each
        value in a numeric column falls within [lower, upper] (inclusive).

        Args:
            col_name (str)
            lower (numeric, optional)
            upper (numeric, optional)

        Returns:
            self: The SparkDataCheck object (enables method chaining).
        """
        # --- numeric types recognised by Spark SQL ---
        numeric_types = {"float", "int", "integer", "bigint", "long", "double"}

        # Look up this column's dtype from the DataFrame schema
        col_dtype = dict(self.df.dtypes).get(col_name)

        # Guard: column must exist and be numeric
        if col_dtype not in numeric_types:
            print(
                f"check_numeric_range: '{col_name}' has dtype '{col_dtype}', "
                f"which is not a supported numeric type {numeric_types}. "
                f"No column appended."
            )
            return self

        # Build the range condition depending on which bounds were supplied.
        # .between(lo, hi) is used when both bounds are present; it propagates
        # NULLs as NULL automatically.  Single-sided checks use plain comparisons
        # (which also propagate NULL correctly in Spark SQL).

        if lower is not None and upper is not None:
            condition = F.col(col_name).between(lower, upper)
        elif lower is not None:
            condition = F.col(col_name) >= lower
        elif upper is not None:
            condition = F.col(col_name) <= upper
        else:
            # Neither bound supplied — every non-NULL value trivially passes,
            # NULLs stay NULL.  Warn the user so the call isn't silently useless.
            print(
                f"check_numeric_range: neither 'lower' nor 'upper' was provided "
                f"for column '{col_name}'. Appending all-True/_in_range column."
            )
            condition = F.lit(True)

        # Append the boolean result column and stay chainable
        new_col = col_name + "_in_range"
        self.df = self.df.withColumn(new_col, condition)
        return self

    def check_string_levels(self, col_name, levels):
        """
        Validates that values in a string column fall within a user-specified
        set of allowable levels.
 
        Args:
            col_name (str): Name of the column to validate.
            levels (list):  List of acceptable string values.
 
        Returns:
            self: The SparkDataCheck object with an appended boolean column,
                  or self unmodified if the column is not a string type.
 
        Notes:
            - Non-string columns trigger a printed message and early return.
            - NULL values in the column produce NULL in the output column.
            - The appended column is named  '<col_name>_level_check'.
        """
        # ── 1. Type guard ────────────────────────────────────────────────────
        col_type = dict(self.df.dtypes).get(col_name)
 
        if col_type != "string":
            print(
                f"Column '{col_name}' is of type '{col_type}', not string. "
                "No check performed."
            )
            return self
 
        # ── 2. Build the check expression ────────────────────────────────────
        # F.col(col_name).isin(*levels) returns:
        #   • True  – value is in the list
        #   • False – value is not in the list
        #   • NULL  – value is NULL  (Spark propagates nulls automatically)
        check_expr = F.col(col_name).isin(*levels)
 
        # ── 3. Append result column and return self ───────────────────────────
        new_col_name = f"{col_name}_in_levels"
        self.df = self.df.withColumn(new_col_name, check_expr)
 
        return self

    def check_missing(self, col_name):
        """
        Validation Method 3: Appends a boolean column indicating whether
        each value in col_name is NULL (True = missing, False = not missing).

        Args:
            col_name (str): Name of the column to check for NULL values.

        Returns:
            self: The DataFrameValidator object (enables method chaining).
        """
        self.df = self.df.withColumn(
            col_name + "_missing",
            F.col(col_name).isNull()
        )
        return self

    def numeric_summary(self, col_name=None, group_by=None):
        """
        Returns min and max of numeric column(s) as a regular pandas DataFrame.

        Args:
            col_name (str, optional): Name of a single numeric column to summarize.
                                      If None, all numeric columns are summarized.
            group_by (str, optional): A single column name to group results by.

        Returns:
            pd.DataFrame or None: Summary table, or None if col_name is not numeric.
        """
        numeric_types = ['int', 'bigint', 'double', 'float', 'long', 'integer']

        # Build a dict of {col_name: dtype} for easy lookup
        col_type_map = dict(self.df.dtypes)

        if col_name is not None:
            # --- Single column path ---
            col_dtype = col_type_map.get(col_name, "")

            # Check if the column type contains any known numeric keyword
            is_numeric = any(num_type in col_dtype for num_type in numeric_types)

            if not is_numeric:
                print(f"Column '{col_name}' is not numeric (type: '{col_dtype}'). "
                      f"Please supply a numeric column.")
                return None

            # Run grouped or ungrouped min/max
            col = self.df[col_name]
            if group_by is not None:
                result = (
                    self.df
                    .groupBy(group_by)
                    .agg(F.min(col), F.max(col))
                    .toPandas()
                )
            else:
                result = (
                    self.df
                    .groupBy()
                    .agg(F.min(col), F.max(col))
                    .toPandas()
                )
            return result

        else:
            # --- All numeric columns path ---
            numeric_cols = [
                c for c, t in self.df.dtypes
                if any(num_type in t for num_type in numeric_types)
            ]

            results = []
            for c in numeric_cols:
                col = self.df[c]
                if group_by is not None:
                    partial = (
                        self.df
                        .groupBy(group_by)
                        .agg(F.min(col), F.max(col))
                        .toPandas()
                    )
                else:
                    partial = (
                        self.df
                        .groupBy()
                        .agg(F.min(col), F.max(col))
                        .toPandas()
                )
                results.append(partial)

            # Merge all per-column results into one clean DataFrame
            if group_by is not None:
                combined = reduce(lambda left, right: pd.merge(left, right, on=group_by), results)
            else:
                combined = reduce(pd.merge, results)

            return combined

    def string_counts(self, col1, col2=None):
        """
        Returns a pandas DataFrame of counts grouped by one or two string columns.

        Args:
            col1 (str): Required. Name of the first (or only) grouping column.
            col2 (str, optional): Name of a second grouping column. Default is None.

        Returns:
            pandas.DataFrame or None: Count summary as a regular pandas DataFrame,
                                      or None if a column type check fails.
        """
        # Build a dict of {column_name: dtype} from the Spark DataFrame schema
        dtype_dict = dict(self.df.dtypes)

        # --- Validate col1 ---
        if dtype_dict.get(col1) != "string":
            print(f"Column '{col1}' is not a string column (found type: '{dtype_dict.get(col1)}').")
            return None

        # --- Validate col2 if supplied ---
        if col2 is not None:
            if dtype_dict.get(col2) != "string":
                print(f"Column '{col2}' is not a string column (found type: '{dtype_dict.get(col2)}').")
                return None

        # --- GroupBy and count ---
        if col2 is None:
            result = (
                self.df
                .groupBy([col1])
                .count()
                .toPandas()
            )
        else:
            result = (
                self.df
                .groupBy([col1, col2])
                .count()
                .toPandas()
            )

        return result