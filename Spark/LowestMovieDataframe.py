from pyspark.sql import SparkSession
from pyspark.sql import Row


def load_movie_name() -> object:

    movie_name = {}

    # movieID/ movieName
    with open('ml-100k/u.item') as f:
        for line in f:
            fields = line.split("|")
            movie_name[int(fields[0])] = fields[1]

    return movie_name


def parseInput(line):
    # userID, movieID, rating
    fields = line.split()
    # Remember to translate data into specific data type
    return Row(movieID = int(fields[1]), rating = float(fields[2]))


if __name__ == '__main__':

    # Run SparkSession
    spark = SparkSession.builder.appName(name = "worstMovie").getOrCreate()

    movie_name = load_movie_name()
    input_data = spark.sparkContext.textFile(name = "hdfs://user/maria_dev/ml-100k/u.data")
    input_data = input_data.map(parseInput)
    input_data = spark.createDataFrame(input_data)
    ratings = input_data.groupBy("movieID").avg("rating")
    counts = input_data.groupBy("movieID").count()
    order_join_data = counts.join(ratings, "movieID").orderBy("avg(rating)").take(10)

    for movie in order_join_data:
        print(movie_name[movie[0]], movie[1], movie[2])

    spark.stop()

