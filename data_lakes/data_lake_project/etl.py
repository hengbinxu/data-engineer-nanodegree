import os, configparser
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import (
    StructField, StructType, StringType, DecimalType,
    IntegerType, LongType, DoubleType, TimestampType
)
from pyspark.sql.functions import col, udf, row_number
from pyspark.sql.functions import (
    year, month, dayofmonth, 
    dayofweek, hour, weekofyear,
)

from utils import timer

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    '''
    Create a spark session with specific configuration that we use on
    the other function.
    '''
    spark = SparkSession\
        .builder\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
        .getOrCreate()
    return spark

@timer
def process_song_data(spark: SparkSession, input_data: str, output_data: str):
    '''
    Extract song_data from input_data, and transforming the data write back to output_data.
    args:
        spark(object): spark session object
        input_data(str): AWS S3 bucket URI or file path
        output_data(str): AWS S3 bucket URI or file path
    '''
    # get filepath to song data filed
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    print(song_data)

    staging_song_schema = StructType([
        StructField('song_id', StringType(), True),
        StructField("title", StringType(), True),
        StructField("year", IntegerType(), False),
        StructField("num_songs", IntegerType(), False),
        StructField("artist_id", StringType(), False),
        StructField("artist_name", StringType(), False),
        StructField("artist_location", StringType(), True),
        StructField("artist_latitude", DecimalType(precision=10, scale=5), True),
        StructField("artist_longitude", DecimalType(precision=10, scale=5), True),
        StructField("duration", DecimalType(precision=20, scale=5), True)
    ])
    # read song data file
    staging_song_df = spark.read.json(song_data, schema=staging_song_schema)

    songs_table_output_path = os.path.join(output_data, 'songs/')
    songs_fields = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_partition_key = ['year', 'artist_id']

    # extract columns to create songs table
    songs_table = staging_song_df.select(songs_fields).dropDuplicates()
    print('Export songs_table data with parquet format to {}'.format(songs_table_output_path))
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy(*songs_partition_key).parquet(songs_table_output_path)
    print('Finished export songs_table to {}'.format(songs_table_output_path))

    artists_table_output_path = os.path.join(output_data, 'artists/')
    artists_selectExpr = [
        'artist_id',
        'artist_name as name',
        'artist_location as location',
        'artist_latitude as latitude',
        'artist_longitude as longitude'
    ]
    # extract columns to create artists table
    artists_table = staging_song_df.selectExpr(artists_selectExpr).dropDuplicates()
    print('Export songs_table data with parquet format to {}'.format(songs_table_output_path))
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(artists_table_output_path)
    print('Finished export songs_table to {}'.format(songs_table_output_path))

@timer
def process_log_data(spark: SparkSession, input_data: str, output_data: str):
    '''
    Extract log_data from input_data, and transforming the data write back to output_data.
    args:
        spark(object): spark session object
        input_data(str): AWS S3 bucket URI or file path
        output_data(str): AWS S3 bucket URI or file path
    '''
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    event_log_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("lastName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", IntegerType(), True),
        StructField("length", DecimalType(precision=10, scale=5), True),
        StructField("level", StringType(), False),
        StructField("location", StringType(), True),
        StructField("method", StringType(), False),
        StructField("page", StringType(), False),
        StructField("registration", DoubleType(), True),
        StructField("sessionId", IntegerType(), False),
        StructField("song", StringType(), False),
        StructField("status", IntegerType(), False),
        StructField("ts", LongType(), False),
        StructField("userAgent", StringType(), False),
        StructField("userId", StringType(), False),
    ])

    # read log data file
    staging_log_df = spark.read.json(log_data, schema=event_log_schema)
    
    # filter by actions for song plays
    staging_log_df = staging_log_df.filter(col('page') == 'NextSong')

    # extract columns for users table 
    users_table_output_path = os.path.join(output_data, 'users/')
    users_selectExpr = [
        'userId as user_id',
        'firstName as first_name',
        'lastName as last_name',
        'gender', 'level'
    ]
    artists_table = staging_log_df.selectExpr(users_selectExpr).dropDuplicates()
    print('Export users_table data with parquet format to {}'.format(users_table_output_path))
    # write users table to parquet files
    artists_table.write.mode('overwrite').parquet(users_table_output_path)
    print('Finished export songs_table to {}'.format(users_table_output_path))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda milli_ts: datetime.fromtimestamp(milli_ts/1000), TimestampType())
    staging_log_df = staging_log_df.withColumn(
        'start_time', get_timestamp(col('ts'))
    ).dropDuplicates()

    staging_log_df = staging_log_df.withColumn('hour', hour(col('start_time')))\
        .withColumn('day', dayofmonth(col('start_time')))\
        .withColumn('week', weekofyear(col('start_time')))\
        .withColumn('month', month(col('start_time')))\
        .withColumn('year', year(col('start_time')))\
        .withColumn('weekday', dayofweek(col('start_time')))
    
    # extract columns to create time table
    time_fields = [
        'start_time', 'hour', 'day', 'week',
        'month', 'year', 'weekday'
    ]
    time_table = staging_log_df.select(time_fields).dropDuplicates()
    
    time_output_path = os.path.join(output_data, 'time/')
    print('Export time_table data with parquet format to {}'.format(time_output_path))
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(time_output_path)
    print('Finished export time_table to {}'.format(time_output_path))

    # read in song data to use for songplays table
    songs_table_path = os.path.join(output_data, 'songs/')
    songs_table = spark.read.parquet(songs_table_path)
    songs_table  = songs_table.select(
        'song_id', 'title', 'duration', col('year').alias('song_year'), 'artist_id'
    )
    # Use window function to generate songplay_id
    _window = Window.orderBy(col('start_time'))

    song_plays_selectExpr = [
        'songplay_id', 'start_time', 'userId as user_id',
        'level', 'song_id', 'artist_id', 'sessionId as session_id',
        'location', 'userAgent as user_agent', 'year', 'month'
    ]
    song_plays_table = staging_log_df.join(
        songs_table, staging_log_df.song == songs_table.title
    ).withColumn('songplay_id', row_number().over(_window))\
    .selectExpr(song_plays_selectExpr).repartition('year', 'month')

    song_plays_output_path = os.path.join(output_data, 'song_plays/')
    print('Export song_play data with parquet format to {}'.format(song_plays_output_path))
    # write songplays table to parquet files partitioned by year and month
    song_plays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(song_plays_output_path)
    print('Finished export song_play_table to {}'.format(song_plays_table))

@timer
def main():
    spark = create_spark_session()
    input_data = 's3a://udacity-dend/'
    output_data = 's3a://sparkify-app-bucket/'
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

if __name__ == '__main__':
    main()
