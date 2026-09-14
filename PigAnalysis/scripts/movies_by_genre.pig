-- 3) Count the Number of Movies by Genre : returns how many movies belong to
--    each genre. A movie with several genres is counted once in each of them.
movies = LOAD 'D:/Desktop/movies_project/data' USING PigStorage('\t') AS
   (movie_id:int, title:chararray, genres:chararray, keywords:chararray, director:chararray,
    release_date:chararray, runtime:int, popularity:double, budget:long, revenue:long);
with_genres = FILTER movies BY genres IS NOT NULL;
-- 'Family|Comedy|Animation' becomes one row per genre
movie_genres = FOREACH with_genres GENERATE movie_id, FLATTEN(TOKENIZE(genres, '|')) AS genre;
genre_group = GROUP movie_genres BY genre;
genre_counts = FOREACH genre_group GENERATE group AS genre, COUNT(movie_genres) AS movies;
sorted_desc = ORDER genre_counts BY movies DESC, genre ASC;
STORE sorted_desc INTO 'D:/Desktop/movies_project/PigAnalysis/output/MoviesByGenre' USING PigStorage('\t');
