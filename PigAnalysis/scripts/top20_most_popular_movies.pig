-- 1) Top 20 Most Popular Movies : returns the 20 movies with the highest TMDb
--    popularity score.
movies = LOAD 'D:/Desktop/movies_project/data' USING PigStorage('\t') AS
   (movie_id:int, title:chararray, genres:chararray, keywords:chararray, director:chararray,
    release_date:chararray, runtime:int, popularity:double, budget:long, revenue:long);
-- movie_id breaks ties, so equal scores always come out in the same order
sorted_desc = ORDER movies BY popularity DESC, movie_id ASC;
top20_movies = LIMIT sorted_desc 20;
-- formatted last: popularity must stay a number while it is being sorted
top20_formatted = FOREACH top20_movies GENERATE movie_id,
                  SPRINTF('%.4f', popularity) AS popularity, title;
STORE top20_formatted INTO 'D:/Desktop/movies_project/PigAnalysis/output/Top20PopularMovies' USING PigStorage('\t');
