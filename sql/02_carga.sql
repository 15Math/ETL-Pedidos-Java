
\copy temp_pedidos_dia FROM '*caminho escondido*' WITH (FORMAT CSV, DELIMITER ';', HEADER, ENCODING 'UTF8');
UPDATE temp_pedidos_dia SET
                            valor = REPLACE(valor, ',', '.'),
                            frete = REPLACE(frete, ',', '.');