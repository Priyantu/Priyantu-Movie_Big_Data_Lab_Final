-- 2) Top 20 Highest-Revenue Movies : returns the 20 movies with the highest
--    box-office revenue.
movies = LOAD 'D:/Desktop/movies_project/data' USING PigStorage('\t') AS
   (movie_id:int, title:chararray, genres:chararray, keywords:chararray, director:chararray,
    release_date:chararray, runtime:int, popularity:double, budget:long, revenue:long);
-- revenue 0 means the amount is unknown
with_revenue = FILTER movies BY revenue > 0;
revenue_only = FOREACH with_revenue GENERATE movie_id, revenue, title;
sorted_desc = ORDER revenue_only BY revenue DESC, movie_id ASC;
top20_movies = LIMIT sorted_desc 20;
STORE top20_movies INTO 'D:/Desktop/movies_project/PigAnalysis/output/Top20RevenueMovies' USING PigStorage('\t');
