from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.ml.recommendation import ALS


def loadMovieNames(path):
    # MovieID, MovieName
    movie_name = {}
    with open("./ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movie_name[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return movie_name


def parseInput(line):
    # UserID | MovieID | Rating
    fields = line.value.split()
    return Row(userID = int(fields[0]), movieID = int(fields[1]), rating = float(fields[2]))


if __name__ == '__main__':

    spark = SparkSession.builder.appName("MovieRecommendation").getOrCreate()

    movie_name = loadMovieNames('./ml-100k/u.item')
    text = spark.read.text('./ml-100k/u.data').rdd  # Remember to add .rdd
    data = text.map(parseInput)

    data_frame = spark.createDataFrame(data).cache()  # Remember to cache to avoid reload

    als = ALS(maxIter = 5, regParam = 0.01, userCol = "userID", itemCol = "movieID", ratingCol = "rating")
    model = als.fit(data_frame)

    user_rating = data_frame.filter("userID = 0")

    rating_count = data_frame.groupBy("movieID").count().filter("count > 100")
    # Create new column
    popular_movie = rating_count.select("movieID").withColumn('userID', lit(0))

    # Use data to predict recommendation using ALS model
    recommendations = model.transform(popular_movie)
    topRecommendation = recommendations.sort(recommendations.prediction.desc()).take(20)

    for rec in topRecommendation:
        print(movie_name[rec['movieID']], rec['prediction'])

    spark.stop()


