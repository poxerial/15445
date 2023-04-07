sqlite>WITH p_avg(person_id, avg_rating) AS (
(x1...>    SELECT people.person_id, AVG(rating) FROM
(x1...>    ratings, crew, people
(x1...>    WHERE ratings.title_id = crew.title_id
(x1...>    AND crew.person_id = people.person_id
(x1...>    AND people.born = 1957
(x1...>    GROUP BY people.person_id
   ...>)
   ...>SELECT name, ROUND(avg_rating, 2) FROM
   ...>p_avg, people
   ...>WHERE p_avg.person_id = people.person_id
   ...>ORDER BY avg_rating DESC, name ASC
   ...>LIMIT 1 OFFSET 8;

Mark Kempner|8.6
