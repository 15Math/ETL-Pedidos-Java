-- O comando \copy resolve o erro de "Permission Denied"
\copy temp_pedidos_dia FROM '*caminho escondido*' WITH (FORMAT CSV, DELIMITER ';', HEADER, ENCODING 'UTF8');

-- Tratamento de dados: Troca vírgula por ponto para permitir conversão numérica posterior
UPDATE temp_pedidos_dia SET
                            valor = REPLACE(valor, ',', '.'),
                            frete = REPLACE(frete, ',', '.');