BEGIN;

-- 1. Clientes (Moda antiga: WHERE NOT EXISTS)
INSERT INTO clientes (codigoComprador, nome, email, endereco, CEP, UF, pais)
SELECT DISTINCT
    t.codigoComprador, t.nomeComprador, t.email, t.endereco, t.CEP, t.UF, t.pais
FROM temp_pedidos_dia t
WHERE NOT EXISTS (SELECT 1 FROM clientes c WHERE c.codigoComprador = t.codigoComprador);

-- 2. Produtos
INSERT INTO produtos (SKU, UPC, nome)
SELECT DISTINCT t.SKU, t.UPC, t.nomeProduto
FROM temp_pedidos_dia t
WHERE NOT EXISTS (SELECT 1 FROM produtos p WHERE p.SKU = t.SKU);

-- 3. Pedidos (Convertendo Texto para Numeric na hora da soma)
INSERT INTO pedidos (codigoPedido, codigoComprador, dataPedido, frete, valor_total)
SELECT
    t.codigoPedido,
    MAX(t.codigoComprador),
    MAX(t.dataPedido),
    MAX(t.frete::NUMERIC),
    SUM(t.valor::NUMERIC * t.qtd) + MAX(t.frete::NUMERIC)
FROM temp_pedidos_dia t
WHERE NOT EXISTS (SELECT 1 FROM pedidos ped WHERE ped.codigoPedido = t.codigoPedido)
GROUP BY t.codigoPedido;

-- 4. Itens da Compra
INSERT INTO compra (codigoPedido, SKU, quantidade, valor_unitario)
SELECT codigoPedido, SKU, qtd, valor::NUMERIC
FROM temp_pedidos_dia;

-- 5. Expedição
INSERT INTO expedicao (codigoPedido, status)
SELECT DISTINCT t.codigoPedido, 'Aguardando Separação'
FROM temp_pedidos_dia t
WHERE NOT EXISTS (SELECT 1 FROM expedicao e WHERE e.codigoPedido = t.codigoPedido);

COMMIT;