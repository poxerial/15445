sqlite> WITH title_ids(title_id) AS (                                           
(x1...> SELECT title_id FROM                                                    
(x1...> crew, people                                                            
(x1...> WHERE crew.person_id = people.person_id                                 
(x1...> AND name = 'Nicole Kidman' AND born = 1967                              
(x1...> )                                                                       
   ...> SELECT DISTINCT name FROM                                               
   ...> crew, people                                                            
   ...> WHERE crew.person_id = people.person_id                                 
   ...> AND (crew.category = 'actor' OR crew.category = 'actress')              
   ...> AND crew.title_id in title_ids                                          
   ...> ORDER BY name ASC;

Betty Gilpin
Casey Affleck
Colin Farrell
Crista Flanagan
Danny Huston
Dennis Miller
Donald Sutherland
Ed Mantell
Fionnula Flanagan
Flora Cross
Fredrik Skavlan
Gus Mercurio
Halle Berry
Harris Yulin
J.K. Simmons
Jackson Bond
James Corden
Jason Bateman
Javier Bardem
Jesper Christensen
John Lithgow
Julianne Moore
Kai Lewins
Kyle Mooney
Lisa Flanagan
Liz Burch
Mahershala Ali
Maria Tran
Mark Strong
Nicholas Eadie
Nicole Kidman
Paul Bettany
Pauline Chan
Robert Pattinson
Russell Crowe
Sam Neill
Shailene Woodley
Sherie Graham
Simon Baker
Stellan Skarsgård
Tom Cruise
Valerie Yu
Veronica Lang
Will Ferrell
