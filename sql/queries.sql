-- =========================================================
-- PLAYSTATION SALES ANALYSIS - 35 SQL QUERIES
-- =========================================================
-- Author: Calin Silucu
-- Dataset: 4,963 PlayStation games (PS3/PS4/PS5)
-- Database: SQLite
-- Query Complexity: Tier 1-4 (Basic to Window Functions)
-- =========================================================


-- =========================================================
-- Q1: Console with Most Games
-- =========================================================
-- Business Question: Which PlayStation console has the largest library?
-- Expected Result: PS4 with 1,991 games
-- Complexity: Tier 1 (Basic)

WITH CTE_most_games as (
    SELECT Console, COUNT(*) as Nr_of_games
    FROM PlayStation_Games
    GROUP By Console
)
SELECT Console, MAX(Nr_of_games) as Most_Games
FROM CTE_most_games;


-- =========================================================
-- Q2: Top 10 games with the most copies sold
-- =========================================================
-- Business Question: The best 10 games with the most copies sold?
-- Expected Result: Games and their total sale
-- Complexity: Tier 1 (Basic)

SELECT Game, [Total Sales]
FROM PlayStation_Games
ORDER BY [Total Sales] DESC
LIMIT 10

-- =========================================================
-- Q3: What are the top 5 best games for each console?
-- =========================================================
-- Business Question: Best 5 games for each console?
-- Expected Result: Console, Game and total sales
-- Complexity: Tier 4 (Window Functions & Advanced SQL)

SELECT Console, Game, [Total Sales]
FROM(
    SELECT Console, Game, [Total Sales],
        ROW_NUMBER() OVER (PARTITION BY Console ORDER BY [Total Sales] DESC) as rank
    FROM PlayStation_Games)
WHERE rank <=5
ORDER BY Console, rank

-- =========================================================
-- Q4: What are the top 15 Publishers based on total sales?
-- =========================================================
-- Business Question: Top 15 Publishers 
-- Expected Result: Publisher and total sales
-- Complexity: Tier 2 (Subqueries & Complex Filters)

SELECT Publisher, SUM([Total Sales]) as All_Sales
    FROM PlayStation_Games
    GROUP BY Publisher
    ORDER BY All_Sales DESC
    LIMIT 15

-- =========================================================
-- Q5: What are the top 10 Developers based on average rating?
-- =========================================================
-- Business Question: Top 10 Developers 
-- Expected Result: Developer and average rating
-- Complexity: Tier 1 (Basic)

SELECT Developer, AVG(rating) as average_rating
    FROM PlayStation_Games
    GROUP BY Developer
    ORDER BY average_rating DESC
    LIMIT 10


-- =========================================================
-- Q6: What developers has the most games made?
-- =========================================================
-- Business Question: Developers with the most games 
-- Expected Result: Developer and number of games
-- Complexity: Tier 1 (Basic)

SELECT Developer, COUNT(Game) as Nr_of_games  
    FROM PlayStation_Games
    GROUP BY DEVELOPER
    HAVING Developer != "Unknown"
    ORDER BY Nr_of_games DESC
    LIMIT 3


-- =========================================================
-- Q7: How many games are in each genre?
-- =========================================================
-- Business Question: Games per genre 
-- Expected Result: Genres and number of games
-- Complexity: Tier 1 (Basic)

SELECT genres, COUNT(Game) as total_games
    FROM PlayStation_Games
    GROUP BY genres
    ORDER BY total_games DESC


-- =========================================================
-- Q8: Which genre have the biggest average rating?
-- =========================================================
-- Business Question: Genres with the biggest average rating 
-- Expected Result: Genres and average rating
-- Complexity: Tier 1 (Basic)

SELECT genres, AVG(rating) as avg_rating
    FROM PlayStation_Games
    GROUP BY genres
    ORDER BY avg_rating DESC
    LIMIT 1 


-- =========================================================
-- Q9: How many games have been launched each year?
-- =========================================================
-- Business Question: Games launched each year
-- Expected Result: Year and number of games
-- Complexity: Tier 1 (Basic)

SELECT [Release Year], COUNT(Game) as Nr_of_games
    FROM PlayStation_Games
    GROUP BY [Release Year]
    ORDER BY Nr_of_games DESC


-- =========================================================
-- Q10: Which genre has the best average metacritic score?
-- =========================================================
-- Business Question: Genre with the best metacritic score
-- Expected Result: Genre and average metacritic score
-- Complexity: Tier 1 (Basic)

SELECT genres, AVG(metacritic) as avg_metacritic
    FROM PlayStation_Games
    GROUP BY genres
    ORDER BY avg_metacritic DESC
    LIMIT 1
    

-- =========================================================
-- Q11: In wich year where the most games sold?
-- =========================================================
-- Business Question: best year on the game market
-- Expected Result: 2014
-- Complexity: Tier 1 (Basic)

SELECT [Release year], SUM([Total Sales]) as all_sales
    FROM PlayStation_Games
    GROUP BY [Release Year]
    ORDER BY all_sales DESC
    LIMIT 1


-- =========================================================
-- Q12: What where the top 5 games launched in 2020?
-- =========================================================
-- Business Question: Best games from 20202
-- Expected Result: Year and average rating
-- Complexity: Tier 1 (Basic)

SELECT [Release year], Game, AVG(rating) as avg_rating
    FROM PlayStation_Games
    WHERE [Release Year] == 2020
    ORDER BY avg_rating DESC
    LIMIT 5


-- =========================================================
-- Q13: Which publisher has the most games with an average rating >= 4?
-- =========================================================
-- Business Question: Publisher with good games
-- Expected Result: Publisher, average rating and number of games
-- Complexity: Tier 1 (Basic)

SELECT Publisher, AVG(rating) as avg_rating, COUNT(Game) as nr_of_games
    FROM PlayStation_Games
    GROUP BY Publisher
    HAVING avg_rating >= 4
    ORDER BY nr_of_games DESC
    LIMIT 1

-- =========================================================
-- Q14: How many games have a rating >= 4 and sold more than 1 million copies?
-- =========================================================
-- Business Question: Good games with a lot of sales
-- Expected Result: Total games: 110
-- Complexity: Tier 2 (Subqueries & Complex Filters)

with cte_games as (SELECT COUNT(Game) as Nr_of_Games, AVG(rating) as avg_rating, [Total Sales]
    FROM PlayStation_Games
    GROUP BY [Total Sales]
    HAVING avg_rating >=4 AND [Total Sales] > 1000000
    ORDER BY avg_rating)

    SELECT COUNT(Nr_of_Games) as total_nr_of_games
    FROM cte_games

-- =========================================================
-- Q15: What is the average sale for games with a metacritic level smaller than 70?
-- =========================================================
-- Business Question: Average sale for games with a score smaller than 70
-- Expected Result: Average sale: 263752.808989
-- Complexity: Tier 2 (Subqueries & Complex Filters)

WITH cte_avg_metacricitc as(SELECT Game,  metacritic, [Total Sales]
    FROM PlayStation_Games
    GROUP BY Game
    HAVING metacritic < 70)

    SELECT AVG([Total Sales]) as avg_sales
    FROM cte_avg_metacricitc


-- =========================================================
-- Q16: What % of sales for each region?
-- =========================================================
-- Business Question: Percentage sale for each region
-- Expected Result: na_pct: 39.157046, eu_pct:38.900571, jpn_pct:7.240913, others_pct:14.710176
-- Complexity: Tier 2 (Subqueries & Complex Filters)

SELECT (SUM([NA Sales])/SUM([Total Sales]))*100 as na_pct, (SUM([PAL Sales])/SUM([Total Sales]))*100 as eu_pct, (SUM([Japan Sales])/SUM([Total Sales]))*100 as jpn_pct, (SUM([Other Sales])/SUM([Total Sales]))*100 as others_pct 
    FROM PlayStation_Games


-- =========================================================
-- Q17: What Publisher has the biggest market share in the NA region?
-- =========================================================
-- Business Question: Publisher having the biggest market share in NA
-- Expected Result: Publisher, total sales, market share from NA region
-- Complexity: Tier 2 (Subqueries & Complex Filters)

SELECT Publisher, SUM([NA Sales]) as Total_sales, (SUM([NA Sales]) / (SELECT SUM([NA Sales]) FROM PlayStation_Games)) * 100 as Market_Share_NA
    FROM PlayStation_Games
    GROUP BY Publisher
    HAVING Market_Share_NA > 0
    ORDER BY Total_sales DESC
    LIMIT 10

-- =========================================================
-- Q18: What Publisher has the biggest market share in the Japan region?
-- =========================================================
-- Business Question: Publisher having the biggest market share in Japan
-- Expected Result: Publisher, total sales, market share from Japan region
-- Complexity: Tier 2 (Subqueries & Complex Filters)

SELECT Publisher, SUM([Japan Sales]) as Total_sales, (SUM([Japan Sales]) / (SELECT SUM([Japan Sales]) FROM PlayStation_Games)) * 100 as Market_Share_Japan
    FROM PlayStation_Games
    GROUP BY Publisher
    HAVING Market_Share_Japan > 0
    ORDER BY Total_sales DESC


-- =========================================================
-- Q19: What is the market share for each console?
-- =========================================================
-- Business Question: Market share for consoles
-- Expected Result: Console, total sales and share_pct
-- Complexity: Tier 2 (Subqueries & Complex Filters)

 SELECT Console, SUM([Total Sales]) as Total_Sales, (SUM([Total Sales])/(SELECT SUM([Total Sales]) FROM PlayStation_Games))*100 as share_Pct 
    FROM PlayStation_Games
    GROUP BY Console
    ORDER BY share_Pct DESC

-- =========================================================
-- Q20: Which are the games with metacritic > 85 but with sales < 500K
-- =========================================================
-- Business Question: Games with good score but few sales
-- Expected Result: Game, metacritic score and total sales
-- Complexity: Tier 1 (Basic)

SELECT Game, metacritic, [Total Sales]
    FROM PlayStation_Games
    WHERE metacritic > 85 AND [Total Sales] < 500000 
    ORDER BY [Total Sales] Desc


-- =========================================================
-- Q21: Which games have more succes in Japan Region than in NA Region?
-- =========================================================
-- Business Question: Games with a better succes in Japan than in NA Region
-- Expected Result: Game, Japan sales and NA Region sales
-- Complexity: Tier 2 (Subqueries & Complex Filters)

SELECT Game, SUM([Japan Sales]) as japan_sales, SUM([NA Sales]) as na_sales
    FROM PlayStation_Games
    GROUP BY Game
    HAVING japan_sales > na_sales
    ORDER BY japan_sales DESC


-- =========================================================
-- Q22: Which publishers has games across all 3 consoles?
-- =========================================================
-- Business Question: Legacy publisher with games across all 3 console
-- Expected Result: Publisher and number of consoles
-- Complexity: Tier 2 (Subqueries & Complex Filters)

SELECT Publisher, COUNT(DISTINCT Console) as nr_console
    FROM PlayStation_Games
    GROUP BY PUBLISHER
    HAVING nr_console = 3

-- =========================================================
-- Q23: How annual sales look for every console?
-- =========================================================
-- Business Question: Annual sales for each console
-- Expected Result: Console, total sales and years
-- Complexity: Tier 3 (Complex Business Logic)

 SELECT Console, SUM([Total Sales]) as total_sales, [Release Year]
    FROM PlayStation_Games
    GROUP BY [Release Year], Console
    ORDER BY Console


-- =========================================================
-- Q24: How many games are launched every year and what is the average sale for those games?
-- =========================================================
-- Business Question: Games every year and average sales for those games
-- Expected Result: Number of games,average sales and years
-- Complexity: Tier 2 (Subqueries & Complex Filters)

SELECT COUNT(*) as nr_games, AVG([Total Sales]) as avg_sales, [Release Year]
    FROM PlayStation_Games
    GROUP BY [Release Year]
    ORDER BY [Release Year]


-- =========================================================
-- Q25: Which Publisher has a better average sales over the years?
-- =========================================================
-- Business Question: Publisher with the best average sales 
-- Expected Result: Publisher,average sales and years
-- Complexity: Tier 2 (Subqueries & Complex Filters)

SELECT Publisher, AVG([Total Sales]) as avg_sales, [Release Year] as year
    FROM PlayStation_Games
    GROUP BY Publisher, year
    ORDER BY year, Publishercc


-- =========================================================
-- Q26: What is the life-cycle of each console? (sales per year from launch)
-- =========================================================
-- Business Question: Life-Cycle of each console
-- Expected Result: Console ,year and average sales
-- Complexity: Tier 3 (Complex Business Logic)

SELECT Console, [Release Year], AVG([Total Sales])
    FROM PlayStation_Games
    GROUP BY Console, [Release year]
    ORDER BY Console, [Release year]


-- =========================================================
-- Q27: Which developer works with the most publishers?
-- =========================================================
-- Business Question: Developer that with the most publishers
-- Expected Result: Developer and number of publishers
-- Complexity: Tier 2 (Subqueries & Complex Filters)

SELECT Developer, COUNT(DISTINCT Publisher) as nr_of_publishers
    FROM PlayStation_Games
    GROUP BY Developer
    HAVING Developer != "Unknown"
    ORDER BY nr_of_publishers DESC
    LIMIT 1


-- =========================================================
-- Q28: Which Publisher has the most diversity of genres in his portofolio?
-- =========================================================
-- Business Question: Publisher with the most diverse games in his portofolio
-- Expected Result: Publisher and number of genres
-- Complexity: Tier 1 (Basic)

    SELECT Publisher, COUNT(DISTINCT genres) as nr_genres
    FROM PlayStation_Games
    GROUP BY Publisher
    HAVING Publisher != "Unknown"
    ORDER BY nr_genres DESC
    LIMIT 1


-- =========================================================
-- Q29: Which combination of genres + console has the most sales?
-- =========================================================
-- Business Question: Combination of genre and console that have the most sales
-- Expected Result: Genre, console and total sales
-- Complexity: Tier 3 (Complex Business Logic)

    SELECT genres, Console, SUM([Total Sales]) as total_sales
    FROM PlayStation_Games
    GROUP BY genres, Console
    ORDER BY total_sales DESC
    LIMIT 1


-- =========================================================
-- Q30: Which combination of Publisher + genre is the most profitable?
-- =========================================================
-- Business Question: Combination of genre and publisher that have the most sales
-- Expected Result: Genre, publisher and total sales
-- Complexity: Tier 3 (Complex Business Logic)

    SELECT genres, Publisher, SUM([Total Sales]) as total_sales
    FROM PlayStation_Games
    GROUP BY Publisher, genres
    ORDER BY total_sales DESC
    LIMIT 1

-- =========================================================
-- Q31: Which games are in top 10% sales overall?
-- =========================================================
-- Business Question: Best sold games
-- Expected Result: Game, Console and total sales
-- Complexity: Tier 3 (Complex Business Logic)

SELECT Game, Console, [Total Sales]
    FROM PlayStation_Games
    ORDER BY [Total Sales] DESC
    LIMIT (SELECT COUNT(*) / 10 FROM PlayStation_Games)


-- =========================================================
-- Q32: For each year, what where the top 3 games? (based on rating)
-- =========================================================
-- Business Question: Top 3 games for each year
-- Expected Result: Game, Console and total sales
-- Complexity: Tier 4 (Window Functions & Advanced SQL)

with cte_rank as(SELECT [Release Year] as Year, rating, Game,
        ROW_NUMBER() OVER(PARTITION BY [Release Year] ORDER BY rating DESC) as rank
    FROM PlayStation_Games)

    SELECT Year, Game, rating
    FROM cte_rank
    WHERE rank <=3
    ORDER BY Year


-- =========================================================
-- Q33: What is the peak year for each publisher? (sales based)
-- =========================================================
-- Business Question: Top 3 games for each year
-- Expected Result: Game, Console and total sales
-- Complexity: Tier 4 (Window Functions & Advanced SQL)

    SELECT Publisher, [Total Sales], [Release Year]
    FROM (SELECT Publisher, [Total Sales], [Release Year],
            ROW_NUMBER() OVER(PARTITION BY Publisher ORDER BY [Total Sales] DESC) as rank
         FROM PlayStation_Games)
    WHERE rank = 1 and [Total Sales] != 0
    ORDER BY Publisher, [Total Sales] DESC


-- =========================================================
-- Q34: What is the efficiency for each publisher? (sales per game released)
-- =========================================================
-- Business Question: Publisher and its efficiency
-- Expected Result: Publisher, all sales, number of games and their efficiency
-- Complexity: Tier 4 (Window Functions & Advanced SQL)

SELECT Publisher, SUM([Total Sales]) all_sales, COUNT(Game) as nr_of_games, ((SUM([Total Sales])/COUNT(Game))*100) as efficiency_per_game
    FROM PlayStation_Games
    GROUP BY Publisher
    HAVING all_sales > 0
    ORDER BY efficiency_per_Game DESC


-- =========================================================
-- Q35: Market leader for each genre (Dominant Publisher)
-- =========================================================
-- Business Question: Most dominant publisher
-- Expected Result: Publisher, total sales and genres
-- Complexity: Tier 4 (Window Functions & Advanced SQL)

SELECT Publisher, genres, total_sales
    FROM (SELECT Publisher, genres, SUM([Total Sales]) as total_sales,
            RANK() OVER(PARTITION BY genres ORDER BY SUM([Total Sales]) DESC) as ranking
        FROM PlayStation_Games
        GROUP BY Publisher, genres
        HAVING total_sales > 0)
    WHERE ranking =1 
    ORDER BY total_sales DESC

