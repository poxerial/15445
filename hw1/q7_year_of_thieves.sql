sqlite> SELECT COUNT(*) FROM titles AS t1
   ...> WHERE t1.premiered IN (
(x1...> SELECT t2.premiered
(x1...> FROM akas, titles AS t2
(x1...> WHERE akas.title_id = t2.title_id
(x1...> AND title = 'Army of Thieves'
(x1...> );

63843
