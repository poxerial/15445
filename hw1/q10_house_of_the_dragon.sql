sqlite>WITH RECURSIVE result(str, id) AS (
(x1...>WITH dubbed_titles(dubbed_title, id) AS (
(x2...>SELECT dubbed_title, ROW_NUMBER() OVER (ORDER BY dubbed_title ASC)
(x2...>FROM (
(x3...>SELECT DISTINCT title AS dubbed_title
(x3...>FROM akas
(x3...>WHERE title_id IN (
(x4...>SELECT DISTINCT title_id FROM akas
(x4...>WHERE title = 'House of the Dragon'
(x3...>)
(x2...>)
(x1...>)
(x1...>SELECT dubbed_title, id FROM dubbed_titles 
(x1...>WHERE id = 1
(x1...>UNION ALL
(x1...>SELECT str || ', ' || dubbed_title, dubbed_titles.id
(x1...>FROM result, dubbed_titles
(x1...>WHERE dubbed_titles.id = result.id + 1 
   ...>)
   ...>SELECT str FROM result
   ...>ORDER BY id DESC
   ...>LIMIT 1;

A Casa do Dragão, A Guerra dos Tronos: A Casa do Dragão, Dom smoka, Game of Thrones: A Casa do Dragão, Gia Tộc Rồng, House of the Dragon, La Casa del Dragón, La casa del dragón, Rod draka, Ród smoka, Sárkányok háza, Zmajeva kuća, Дом Дракон
 , Домът на дракона, Дім Дракона, Кућа змаја, בית הדרקון, آل التنين, ハウス・オ
  ・ザ・ドラゴン, 龍族前傳, 하우스 오브 드래곤