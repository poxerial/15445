sqlite> SELECT name, COUNT(*) AS num_appearances                                   ...> FROM crew, people                       
   ...> WHERE crew.person_id = people.person_id
   ...> GROUP BY crew.person_id
   ...> ORDER BY num_appearances DESC
   ...> LIMIT 20;

Nivedita Basu|12564
Joel Beckerman|11286
Vic Sotto|10626
Ed McMahon|10153
Sameera Sherief|9905
Johnny Gilbert|9677
Erwin Romulo|8774
Ernesto Alonso|8471
Go Haruna|8454
Bradley Bell|8262
Helio Bannwart|7429
Danie Joubert|7340
Ginji Itô|7059
Shôichi Kuroki|7053
Duma Ndlovu|6865
Vaidehi Ramamurthy|6806
Sharad Tripathi|6649
Adrián Suar|5939
Mrinal Jha|5869
Jada Rowland|5763