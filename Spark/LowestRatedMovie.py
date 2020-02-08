from pyspark import SparkConf, SparkContext

def loadMovieName():
    movieNames = {}
    with open('ml-100k/u.item') as f:
        for line in f:
            fields = line.split('|')
        # fields[0] = movieID
            movieNames[int(fields[0])] = fields[1]

    return movieNames

def parseInputs(line):
    # UserID / MovieID / Ratings / Timestamp
    fields = line.split()
    return(int(fields[1]), (float(fields[2]), 1.0))

if __name__ == '__main__':
    conf = SparkConf().setAppName('WorstMovies')
    sc = SparkContext(conf = conf)

    movieNames = loadMovieName()
    line = sc.textFile('ml-100k/u.data')
    # Convert into (movieID, (ratings, 1))
    movieRatings = line.map(parseInputs)
    movieRatingsCount = movieRatings.reduceByKey(lambda movie1, movie2: (movie1[0] + movie2[0],
                                                                        movie1[1] + movie2[1]))
    # Map to (rating, averageRatings)
    averageRatings = movieRatingsCount.mapValues(lambda x: x[0]/x[1])
    sortedMovies = averageRatings.sortBy(lambda x: x[1])
    results = sortedMovies.take(10)

    for result in results:
        print(movieNames[result[0]], result[1])
