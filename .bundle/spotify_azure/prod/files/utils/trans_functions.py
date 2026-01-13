from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim

class DataCleaner:
    
    def rename_columns(self, df: DataFrame, rename_map: dict) -> DataFrame:
        """
        Rename columns using rename_map.
        Columns not present in rename_map remain unchanged.
        """

        return df.select(
            *[
                df[c].alias(rename_map.get(c, c))
                for c in df.columns
            ]
        )
    def drop_columns(self, df: DataFrame, columns: list) -> DataFrame:
        """
        Drop one or multiple columns from a DataFrame.
        Args:
            df: Input Spark DataFrame
            columns: List of column names to drop
        Returns:
            A new DataFrame without the specified columns
        """
        return df.drop(*columns)
    
    def remove_duplicates(self, df: DataFrame, cols=None) -> DataFrame:
        """
        Remove duplicate rows.
        If cols is None, full row duplicates are removed.
        """
        return df.dropDuplicates(cols) if cols else df.dropDuplicates()
    
    def drop_nulls(self, df: DataFrame, cols: list) -> DataFrame:
        """
        Drop rows where any of the given columns are null.
        """
        return df.dropna(subset=cols)
    def trim_strings(self, df: DataFrame) -> DataFrame:
        """
        Trim leading and trailing spaces from all string columns.
        """
        string_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string"]

        for c in string_cols:
            df = df.withColumn(c,trim(col(c)))

        return df