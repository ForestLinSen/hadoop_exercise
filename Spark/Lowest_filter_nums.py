from pyspark.sql import SparkSession
from pyspark.sql import Row


def movie_names(path):
    name_dict = {}
    with open(path) as f:
        for line in f:
            fields = line.split('|')
            name_dict[int(fields[0])] = fields[1]

    return name_dict

def parseInputs(file):
    fields = file.value.split()
    return Row(userID = int(fields[0]),
                movieID = int(fields[1]),
               rating = float(fields[2]))


if __name__ == '__main__':

    spark = SparkSession.builder.appName("Lowest_filter").getOrCreate()

    movie_dict = movie_names('ml-100k/u.item')
    text = spark.read.text('ml-100k/u.data').rdd

    inputs = text.map(parseInputs)
    data_frame = spark.createDataFrame(inputs).cache()

    ratings = data_frame.groupBy("movieID").avg("rating")
    count = data_frame.groupBy("movieID").count()

    result = count.join(ratings, "movieID").filter("count > 10").orderBy("avg(rating)").take(10)

    for r in result:
        print(movie_dict[r[0]], r[1], r[2])

