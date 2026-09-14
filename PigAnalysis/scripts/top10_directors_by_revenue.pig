-- 5) Top 10 Directors by Total Revenue : returns the ten directors whose movies
--    earned the most box-office revenue, with how many of their movies count.
movies = LOAD 'D:/Desktop/movies_project/data' USING PigStorage('\t') AS
   (movie_id:int, title:chararray, genres:chararray, keywords:chararray, director:chararray,
    release_date:chararray, runtime:int, popularity:double, budget:long, revenue:long);
-- only movies with a known revenue and a named director
with_revenue = FILTER movies BY revenue > 0 AND director IS NOT NULL;
director_group = GROUP with_revenue BY director;
director_revenue = FOREACH director_group GENERATE group AS director,
                   COUNT(with_revenue) AS movies, SUM(with_revenue.revenue) AS total_revenue;
sorted_desc = ORDER director_revenue BY total_revenue DESC, director ASC;
top10_directors = LIMIT sorted_desc 10;
STORE top10_directors INTO 'D:/Desktop/movies_project/PigAnalysis/output/Top10DirectorsByRevenue' USING PigStorage('\t');
