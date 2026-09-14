-- 4) Count the Number of Movies Released per Decade : returns how many movies
--    were released in each decade.
movies = LOAD 'D:/Desktop/movies_project/data' USING PigStorage('\t') AS
   (movie_id:int, title:chararray, genres:chararray, keywords:chararray, director:chararray,
    release_date:chararray, runtime:int, popularity:double, budget:long, revenue:long);
with_date = FILTER movies BY release_date IS NOT NULL;
-- '1995-11-22' -> 1995 -> 1990 -> '1990s'
movie_decades = FOREACH with_date GENERATE
                CONCAT((chararray)((int)SUBSTRING(release_date, 0, 4) / 10 * 10), 's') AS decade;
decade_group = GROUP movie_decades BY decade;
decade_counts = FOREACH decade_group GENERATE group AS decade, COUNT(movie_decades) AS movies;
-- every label has four digits and an 's', so text order is also time order
sorted_asc = ORDER decade_counts BY decade ASC;
STORE sorted_asc INTO 'D:/Desktop/movies_project/PigAnalysis/output/MoviesByDecade' USING PigStorage('\t');
