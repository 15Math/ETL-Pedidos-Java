-- Tabelas Definitivas
CREATE TABLE IF NOT EXISTS clientes (
                                        codigoComprador VARCHAR(50) PRIMARY KEY,
    nome VARCHAR(255), email VARCHAR(255), endereco VARCHAR(255),
    CEP VARCHAR(20), UF VARCHAR(2), pais VARCHAR(50)
    );

CREATE TABLE IF NOT EXISTS produtos (
                                        SKU VARCHAR(50) PRIMARY KEY, UPC VARCHAR(50), nome VARCHAR(255)
    );

CREATE TABLE IF NOT EXISTS pedidos (
                                       codigoPedido VARCHAR(50) PRIMARY KEY,
    codigoComprador VARCHAR(50) REFERENCES clientes(codigoComprador),
    dataPedido DATE, frete NUMERIC(10,2), valor_total NUMERIC(10,2)
    );

CREATE TABLE IF NOT EXISTS compra (
                                      id SERIAL PRIMARY KEY,
                                      codigoPedido VARCHAR(50) REFERENCES pedidos(codigoPedido),
    SKU VARCHAR(50) REFERENCES produtos(SKU),
    quantidade INT, valor_unitario NUMERIC(10,2)
    );

CREATE TABLE IF NOT EXISTS expedicao (
                                         id SERIAL PRIMARY KEY,
                                         codigoPedido VARCHAR(50) REFERENCES pedidos(codigoPedido),
    status VARCHAR(50) DEFAULT 'Aguardando Separação'
    );

-- Apaga a tabela antiga se ela existir para garantir a nova estrutura com TEXT
DROP TABLE IF EXISTS temp_pedidos_dia;

-- Tabela de Staging (Temporária) - Campos financeiros como TEXT para aceitar a vírgula do arquivo
CREATE TABLE temp_pedidos_dia (
                                  codigoPedido VARCHAR(50), dataPedido DATE, SKU VARCHAR(50),
                                  UPC VARCHAR(50), nomeProduto VARCHAR(255), qtd INT,
                                  valor TEXT, frete TEXT, email VARCHAR(255),
                                  codigoComprador VARCHAR(50), nomeComprador VARCHAR(255),
                                  endereco VARCHAR(255), CEP VARCHAR(20), UF VARCHAR(2), pais VARCHAR(50)
);