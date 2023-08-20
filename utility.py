import psycopg2


def rename_columns(df, columns: dict):
        new_df = df
        for key, value in columns.items():
            new_df = new_df.withColumnRenamed(key, value)
        
        return new_df


