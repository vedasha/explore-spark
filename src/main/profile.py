from pyspark.sql.functions import isnan, when, count, col, countDistinct

def profile_dataframe(df):

    columns = df.columns

    # get general statistics provided by spark
    # it will have 5 rows for count, mean, stddev, min, max
    # each column will have those 5 values
    stats = df.describe().collect()

    ## get either nan or null counts
    nan_null_columns = [count(when(isnan(c) | col(c).isNull()), c).alias(c) for c in columns]
    nan_null_counts = df.select(*nan_null_columns)

    ## get distinct value counts
    distinct_columns = [countDistinct(col(c)).alias(c) for c in columns]
    disinct_counts = df.select(*distinct_columns)

    format_string = "%-30s %12s %12s %12s %12s %12s %12s %12s"

    print(format_string % ("column", "count", "count", "stddev", "min", "max", "null", "distinct count"))

    for i in range(len(columns)):
        print(format_string %
              (column[i][:30],
               str(stats[0][i])[:13],
               str(stats[0][i])[:13],
               str(stats[0][i])[:13],
               str(stats[0][i])[:13],
               str(stats[0][i])[:13],
               str(nan_null_counts[i]),
               str(disinct_counts[i])))
