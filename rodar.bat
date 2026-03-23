@echo off
set PGPASSWORD=
set PATH=%PATH%;C:\Program Files\PostgreSQL\18\bin
echo [1/3] Criando estrutura...
psql -U postgres -d meu_banco -f sql/01_schema.sql
echo [2/3] Carregando arquivo TXT...
psql -U postgres -d meu_banco -f sql/02_carga.sql
echo [3/3] Processando regras de negocio...
psql -U postgres -d meu_banco -f sql/03_processamento.sql
pause