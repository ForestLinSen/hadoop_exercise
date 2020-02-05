ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID: int, movieID: int, rating: int, 
													ratingTime: int);
metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|') AS ( movieID: int,
											movieTitle: chararray, releaseDate: chararray,
                                            videoRelease: chararray, imdbLink: chararray);
                                            
movie_title = FOREACH metadata GENERATE movieID, movieTitle;

group_ratings = GROUP ratings BY movieID;

avg_calculate = FOREACH group_ratings GENERATE group AS movieID, AVG(ratings.rating) AS avgRating,
												COUNT(ratings.rating) AS count_num;
                                                
filter_calculate = FILTER avg_calculate BY avgRating < 2.0;

/* Join */
rating_with_title = JOIN filter_calculate BY movieID, movie_title BY movieID;

results = FOREACH rating_with_title GENERATE movie_title::movieTitle AS movieName, 
											filter_calculate::avgRating AS avgRating,
                                            filter_calculate::count_num AS numRatings;

/* Order */
order_worst_with_year = ORDER results BY numRatings DESC;

DUMP order_worst_with_year;
