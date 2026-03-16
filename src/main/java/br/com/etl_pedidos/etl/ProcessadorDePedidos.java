package br.com.etl_pedidos.etl;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import java.io.FileReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class ProcessadorDePedidos {

    private static final String URL = "jdbc:postgresql://localhost:5432/meu_banco";
    private static final String USER = "postgres";
    private static final String PASS = "senha123";

    public static void main(String[] args) {
        System.out.println("Iniciando o sistema de ETL...");

        try (Connection conn = DriverManager.getConnection(URL, USER, PASS)) {

            System.out.println("1. Conectado ao banco! Preparando tabelas...");
            prepararTabelas(conn);

            System.out.println("2. Lendo arquivo TXT e carregando dados...");
            // Verifica se o arquivo está na pasta correta do projeto
            carregarDadosDoCSV(conn, "src/main/resources/pedidos.txt");

            System.out.println("3. Processando regras de negócio...");
            processarRegrasDeNegocio(conn);

            verDadosNoConsole(conn);
            System.out.println("Processamento concluído com sucesso!");

        } catch (Exception e) {
            System.err.println("Erro durante o processamento:");
            e.printStackTrace();
        }


    }
    private static void verDadosNoConsole(Connection conn) throws Exception {
        System.out.println("\n--- DADOS DA TABELA DE PEDIDOS ---");
        try (Statement stmt = conn.createStatement();
             var rs = stmt.executeQuery("SELECT * FROM pedidos")) {

            while (rs.next()) {
                System.out.println("Pedido: " + rs.getString("codigoPedido") +
                        " | Cliente: " + rs.getString("codigoComprador") +
                        " | Frete: R$ " + rs.getBigDecimal("frete") +
                        " | Total: R$ " + rs.getBigDecimal("valor_total"));
            }
        }
    }

    private static void prepararTabelas(Connection conn) throws Exception {
        String sql = """
            -- 1. Cria as tabelas oficiais se elas não existirem
            CREATE TABLE IF NOT EXISTS clientes (
                codigoComprador VARCHAR(50) PRIMARY KEY,
                nome VARCHAR(255),
                email VARCHAR(255),
                endereco VARCHAR(255),
                CEP VARCHAR(20),
                UF VARCHAR(2),
                pais VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS produtos (
                SKU VARCHAR(50) PRIMARY KEY,
                UPC VARCHAR(50),
                nome VARCHAR(255)
            );

            CREATE TABLE IF NOT EXISTS pedidos (
                codigoPedido VARCHAR(50) PRIMARY KEY,
                codigoComprador VARCHAR(50) REFERENCES clientes(codigoComprador),
                dataPedido DATE,
                frete NUMERIC(10,2),
                valor_total NUMERIC(10,2)
            );

            CREATE TABLE IF NOT EXISTS compra (
                id SERIAL PRIMARY KEY,
                codigoPedido VARCHAR(50) REFERENCES pedidos(codigoPedido),
                SKU VARCHAR(50) REFERENCES produtos(SKU),
                quantidade INT,
                valor_unitario NUMERIC(10,2)
            );

            CREATE TABLE IF NOT EXISTS expedicao (
                id SERIAL PRIMARY KEY,
                codigoPedido VARCHAR(50) REFERENCES pedidos(codigoPedido),
                status VARCHAR(50)
            );

            -- 2. Recria a tabela temporária limpa para os dados do dia
            DROP TABLE IF EXISTS temp_pedidos_dia;
            CREATE TABLE temp_pedidos_dia (
                codigoPedido VARCHAR(50),
                dataPedido DATE,
                SKU VARCHAR(50),
                UPC VARCHAR(50),
                nomeProduto VARCHAR(255),
                qtd INT,
                valor NUMERIC(10,2),
                frete NUMERIC(10,2),
                email VARCHAR(255),
                codigoComprador VARCHAR(50),
                nomeComprador VARCHAR(255),
                endereco VARCHAR(255),
                CEP VARCHAR(20),
                UF VARCHAR(2),
                pais VARCHAR(50)
            );
        """;
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    private static void carregarDadosDoCSV(Connection conn, String caminhoArquivo) throws Exception {
        String sqlInsert = "INSERT INTO temp_pedidos_dia VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        // Ensina o leitor a usar ponto e vírgula
        CSVParser parser = new CSVParserBuilder().withSeparator(';').build();

        try (CSVReader reader = new CSVReaderBuilder(new FileReader(caminhoArquivo)).withCSVParser(parser).build();
             PreparedStatement pstmt = conn.prepareStatement(sqlInsert)) {

            String[] linha;
            reader.readNext(); // Pula o cabeçalho do TXT

            while ((linha = reader.readNext()) != null) {
                pstmt.setString(1, linha[0]);
                pstmt.setDate(2, java.sql.Date.valueOf(linha[1]));
                pstmt.setString(3, linha[2]);
                pstmt.setString(4, linha[3]);
                pstmt.setString(5, linha[4]);
                pstmt.setInt(6, Integer.parseInt(linha[5]));

                // Converte vírgula para ponto no valor e frete
                pstmt.setBigDecimal(7, new BigDecimal(linha[6].replace(",", ".")));
                pstmt.setBigDecimal(8, new BigDecimal(linha[7].replace(",", ".")));

                pstmt.setString(9, linha[8]);
                pstmt.setString(10, linha[9]);
                pstmt.setString(11, linha[10]);
                pstmt.setString(12, linha[11]);
                pstmt.setString(13, linha[12]);
                pstmt.setString(14, linha[13]);
                pstmt.setString(15, linha[14]);

                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }

    private static void processarRegrasDeNegocio(Connection conn) throws Exception {
        try (Statement stmt = conn.createStatement()) {

            //Salvando os dados
            stmt.execute("""
                INSERT INTO clientes (codigoComprador, nome, email, endereco, CEP, UF, pais)
                SELECT DISTINCT codigoComprador, nomeComprador, email, endereco, CEP, UF, pais
                FROM temp_pedidos_dia
                ON CONFLICT (codigoComprador) DO NOTHING;
            """);

            stmt.execute("""
                INSERT INTO produtos (SKU, UPC, nome)
                SELECT DISTINCT SKU, UPC, nomeProduto
                FROM temp_pedidos_dia
                ON CONFLICT (SKU) DO NOTHING;
            """);

            stmt.execute("""
                INSERT INTO pedidos (codigoPedido, codigoComprador, dataPedido, frete, valor_total)
                SELECT
                    codigoPedido,
                    MAX(codigoComprador),
                    MAX(dataPedido),
                    MAX(frete),
                    SUM(valor * qtd) + MAX(frete)
                FROM temp_pedidos_dia
                GROUP BY codigoPedido
                ON CONFLICT (codigoPedido) DO NOTHING;
            """);

            stmt.execute("""
                INSERT INTO compra (codigoPedido, SKU, quantidade, valor_unitario)
                SELECT codigoPedido, SKU, qtd, valor
                FROM temp_pedidos_dia;
            """);

            stmt.execute("""
                INSERT INTO expedicao (codigoPedido, status)
                SELECT DISTINCT codigoPedido, 'Aguardando Separação'
                FROM temp_pedidos_dia;
            """);
        }
    }

}