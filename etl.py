import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    - Creates spark session and returns spark object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark,input_data, output_data):
    """
    - Loads song data from s3 into spark
    
    - Processes s3 data
    
    - Writes processed data into parquet files
    
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/A/A/*'
    
    # read the file
    print("Reading song_data....")
    df = spark.read.format('json').load(song_data)
    
    # create temp view 
    df.createOrReplaceTempView("song_data")
    
    # extract columns to create songs table
    songs_table = spark.sql('''
                            SELECT DISTINCT song_id,
                                    title,
                                    artist_id,
                                    year,
                                    duration 
                            
                            FROM song_data
                            ''')
    
    # write songs table to parquet files partitioned by year and artist
    print("Writing songs_table to parquet file.....")
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data+"songs_table", 'overwrite')
    
    # extract columns to create artists table
    artists_table = spark.sql('''
                                SELECT DISTINCT artist_id,
                                    artist_name as name,
                                    artist_location as location,
                                    artist_latitude as latitude,
                                    artist_longitude as longitude
                                
                                FROM song_data
                                ''')
    
    # write artists table to parquet files
    print("Writing artist_table to parquet file.....")
    artists_table.write.parquet(output_data+"artists_table", 'overwrite')

def process_log_data(spark, input_data, output_data):
    """
    - Loads log data from s3 into spark
     
    - Processes the data 
    
    - Writes processed data into parquet files
    """
    # get filepath to log data file
    print("Reading log_data....")
    log_data =  input_data+ 'log_data/*/*'
    
    # read log data file
    df = spark.read.format('json').load(log_data)
    
    # filter by actions for song plays
    df.createOrReplaceTempView("log_data")
    df = spark.sql('''
                    SELECT * 
                    
                    FROM log_data
                    WHERE page= "NextSong"
                    ''')
    
    # extract columns for users table  
    df.createOrReplaceTempView("log_data")
    users_table = spark.sql('''
                              SELECT DISTINCT userId as user_id,
                                  firstName as first_name,
                                  lastName as last_name,
                                  gender,
                                  level
                               
                              FROM log_data
                                    
                               ''')
    
    
    # write users table to parquet files
    print("Writing users_table to parquet....")
    users_table.write.parquet(output_data+"users_table", 'overwrite')
    
   
    # extract columns to create time table
    time_table = spark.sql('''
                            SELECT DISTINCT start_time,
                                HOUR(start_time) AS hour,
                                DAY(start_time) AS day,
                                WEEKOFYEAR(start_time) AS week,
                                MONTH(start_time) AS month,
                                YEAR(start_time) AS year,
                                WEEKDAY(start_time) AS weekday
                                
                            FROM 
                            (SELECT CAST(CAST(ts AS FLOAT)/1000 AS TIMESTAMP) AS start_time
                            FROM log_data)
                            ''')
    
    
    # write time table to parquet files partitioned by year and month
    print("Writing time_table to parquet....")
    time_table.write.partitionBy('year', 'month').parquet(output_data+'time_table', 'overwrite')
    
    
    # read in song data to use for songplays table
    song_data = input_data+'song_data/A/A/*'
    # read the file
    df = spark.read.format('json').load(song_data)
    
    # create temp view 
    df.createOrReplaceTempView("song_data")
    
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
                                WITH log_data1 AS(
                                    SELECT *, start_time FROM
                                    (SELECT *, CAST(CAST(ts AS FLOAT)/1000 AS TIMESTAMP) AS start_time
                                    FROM log_data))
                                    
                                SELECT l.start_time,
                                    MONTH(l.start_time) AS month,
                                    YEAR(l.start_time) AS year,
                                    l.userId AS user_id,
                                    l.level,
                                    s.song_id,
                                    s.artist_id,
                                    l.sessionId as session_id,
                                    l.location,
                                    l.userAgent as user_agent
                                FROM song_data AS s
                                LEFT join log_data1 AS l
                                ON l.artist = s.artist_name AND l.song = s.title
                                WHERE l.page = "NextSong"
                                
                                ''')
    
    
    # write songplays table to parquet files partitioned by year and month
    print("Writing songplays_table to parquet....")
    songplays_table.write.partitionBy('year', 'month').parquet(output_data+"songplays_table", 'overwrite')
    
    

def main():
    """
    - Main function
    
    - Calls functions defined earlier
    
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://spark-data-lake-proj4/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
