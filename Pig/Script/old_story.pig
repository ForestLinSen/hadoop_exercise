/*Create relation*/
ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, 
													ratingTime:int);
/*different delimiter*/                                                    
metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|') AS (movieID: int, 
	movieTitle: chararray, releaseDate: chararray, videoRelease: chararray, imdbLink: chararray);

/* MovieID-MovieTitle-ReleaseTime */ 
nameLookup = FOREACH metadata GENERATE movieID, movieTitle,
	ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) AS releaseTime;
                               
/* */ 
ratingsByMovie = GROUP ratings BY movieID;

/* */ 
avgRatings = FOREACH ratingsByMovie GENERATE group as movieID, AVG(ratings.rating) as avgRating;

/* Filter */
fiveStarMovies = FILTER avgRatings BY avgRating > 4.0;

/* Join */
fiveStarsWithData = JOIN fiveStarMovies BY movieID, nameLookup BY movieID;

/* Order by */
oldestFiveStarMovies = ORDER fiveStarsWithData BY nameLookup::releaseTime;

DUMP oldestFiveStarMovies;

