sqlite> SELECT title, votes
   ...> FROM ratings, akas, crew, people
   ...> WHERE ratings.title_id = akas.title_id
   ...> AND akas.title_id = crew.title_id
   ...> AND crew.person_id = people.person_id
   ...> AND name LIKE '%Cruise%'
   ...> AND born = 1962
   ...> ORDER BY votes DESC
   ...> LIMIT 10;
   
Oblivion|520383
Zaborav|520383
Niepamięć|520383
Oblivion: El tiempo del olvido|520383
Oblivion|520383
オブリビオン|520383
Oblivion|520383
Oblivion|520383
Yi Luo Zhan Jing|520383
Avadon|520383