from pathlib import Path
from datetime import datetime
from src.utils.db_utils import DuckDBConnection


class BronzeTransformer:
    def __init__(self, db_connection: 'DuckDBConnection', config: dict) -> None:
        self.db_connection = db_connection
        self.config = config


    def create_tables(self) -> None:
        """Cria as tabelas necessárias no esquema Bronze."""
        self.db_connection.execute("""
            CREATE SCHEMA IF NOT EXISTS bronze;
        """)

        self.db_connection.execute("""
            CREATE TABLE IF NOT EXISTS bronze.tempo AS
            SELECT
                row_number() OVER () AS id,
                generate_series AS data
            FROM generate_series(DATE '2010-01-01', CURRENT_DATE(), INTERVAL 1 DAY)
        """)

        self.db_connection.execute("""
            CREATE OR REPLACE TABLE bronze.negociacoes (
                id INTEGER PRIMARY KEY,
                usuario_id INTEGER,
                tipo_ativo VARCHAR,
                ticker VARCHAR,
                data_movimentacao DATE,
                quantidade INTEGER,
                tipo_acao VARCHAR,
                valor FLOAT,
                extracted_date DATE
            );
        """)

        self.db_connection.execute("""
            CREATE OR REPLACE TABLE bronze.usuarios (
                id INTEGER PRIMARY KEY,
                nome VARCHAR,
                email VARCHAR UNIQUE,
                extracted_date DATE               
            );
        """)

        self.db_connection.execute("""
            CREATE TABLE IF NOT EXISTS bronze.brapi_quote_list (
                id INTEGER PRIMARY KEY,
                ticker VARCHAR,
                name VARCHAR,
                close FLOAT,
                change FLOAT,
                volume INTEGER,
                market_cap FLOAT,
                logo VARCHAR,
                sector VARCHAR,
                type VARCHAR,
                extracted_date DATE
            );
        """)

        self.db_connection.execute("""
            CREATE TABLE IF NOT EXISTS bronze.fundamentus_resultado (
                id INTEGER PRIMARY KEY,
                ticker VARCHAR,
                cotacao FLOAT,
                p_l FLOAT,
                p_vp FLOAT,
                psr FLOAT,
                dividend_yield FLOAT,
                p_ativo FLOAT,
                p_capital_giro FLOAT,
                p_ebit FLOAT,
                p_ativo_circ_liq FLOAT,
                ev_ebit FLOAT,
                ev_ebitda FLOAT,
                mrg_ebit FLOAT,
                mrg_liquida FLOAT,
                liquidez_corr FLOAT,
                roic FLOAT,
                roe FLOAT,
                liquidez_2_meses FLOAT,
                patrimonio_liquido FLOAT,
                div_bruta_patrim FLOAT,
                cres_rec_5a FLOAT,
                extracted_date DATE
            );
        """)


    def transform(self) -> None:
        landing_path = Path(self.config['paths']['landing'])

        self.db_connection.execute(f"""
            INSERT INTO bronze.tempo
            WITH max_id AS (
                SELECT 
                    MAX(id) AS max_id
                FROM bronze.tempo
            ),
            max_date AS (
                SELECT
                    MAX(data) AS max_date
                FROM bronze.tempo
            )
            SELECT
                row_number() OVER () + (SELECT * FROM max_id) AS id,
                CURRENT_DATE() AS data
            WHERE CURRENT_DATE() > (SELECT * FROM max_date)
        """)

        self.db_connection.execute(f"""
            INSERT INTO bronze.usuarios
            SELECT
                id,
                nome,
                email,
                _extracted_date AS extracted_date
            FROM read_parquet('{landing_path}/sheets/usuarios/usuarios.parquet')
        """)

        self.db_connection.execute(f"""
            INSERT INTO bronze.negociacoes
            SELECT
                row_number() OVER () AS id,
                usuario_id,
                tipo_ativo,
                ticker,
                data_movimentacao,
                quantidade,
                tipo_acao,
                valor,
                _extracted_date AS extracted_date
            FROM read_parquet('{landing_path}/sheets/negociacoes/*.parquet')
        """)

        self.db_connection.execute(f"""
            INSERT INTO bronze.brapi_quote_list
            WITH max_id AS (
                SELECT COALESCE(MAX(id), 0) AS max_id
                FROM bronze.brapi_quote_list
            )
            SELECT
                row_number() OVER () + (SELECT max_id FROM max_id) AS id,
                new.stock AS ticker,
                new.name,
                new.close,
                new.change,
                new.volume,
                new.market_cap,
                new.logo,
                new.sector,
                new.type,
                new._extracted_date AS extracted_date
            FROM read_parquet('{landing_path}/brapi/{datetime.now().strftime("%Y-%m-%d")}.parquet') AS new
            LEFT JOIN bronze.brapi_quote_list AS old
                ON new.stock = old.ticker
                AND new._extracted_date = old.extracted_date
            WHERE old.id IS NULL
        """)
        
        self.db_connection.execute(f"""
            INSERT INTO bronze.fundamentus_resultado
            WITH max_id AS (
                SELECT COALESCE(MAX(id), 0) AS max_id
                FROM bronze.fundamentus_resultado
            )
            SELECT
                row_number() OVER () + (SELECT max_id FROM max_id) AS id,
                new.papel AS ticker,
                new.cotação AS cotacao,  -- Ajuste o nome da coluna aqui
                new.p_l,
                new.p_vp,
                new.psr,
                CAST(REPLACE(REPLACE(REPLACE(new.divyield, '.', ''), ',', '.'), '%', '') AS FLOAT) / 100 AS dividend_yield,  -- Ajuste o nome da coluna aqui
                new.p_ativo,
                new.p_capgiro AS p_capital_giro,  -- Ajuste o nome da coluna aqui
                new.p_ebit,
                new.p_ativ_circliq AS p_ativo_circ_liq,  -- Ajuste o nome da coluna aqui
                new.ev_ebit,
                new.ev_ebitda,
                CAST(REPLACE(REPLACE(REPLACE(new.mrg_ebit, '.', ''), ',', '.'), '%', '') AS FLOAT) / 100 AS mrg_ebit,  -- Ajuste o nome da coluna aqui
                CAST(REPLACE(REPLACE(REPLACE(new.mrg_líq, '.', ''), ',', '.'), '%', '') AS FLOAT) / 100 AS mrg_liquida,  -- Ajuste o nome da coluna aqui
                new.liq_corr AS liquidez_corr,  -- Ajuste o nome da coluna aqui
                CAST(REPLACE(REPLACE(REPLACE(new.roic, '.', ''), ',', '.'), '%', '') AS FLOAT) / 100 AS roic,
                CAST(REPLACE(REPLACE(REPLACE(new.roe, '.', ''), ',', '.'), '%', '') AS FLOAT) / 100 AS roe,
                new.liq2meses AS liquidez_2_meses,  -- Ajuste o nome da coluna aqui
                new.patrim_líq AS patrimonio_liquido,  -- Ajuste o nome da coluna aqui
                new.dívbrut__patrim AS div_bruta_patrim,  -- Ajuste o nome da coluna aqui
                CAST(REPLACE(REPLACE(REPLACE(new.cresc_rec5a, '.', ''), ',', '.'), '%', '') AS FLOAT) / 100 AS cres_rec_5a,  -- Ajuste o nome da coluna aqui
                new._extracted_date AS extracted_date
            FROM read_parquet('{landing_path}/fundamentus/{datetime.now().strftime("%Y-%m-%d")}.parquet') AS new
            LEFT JOIN bronze.fundamentus_resultado AS old
                ON new.papel = old.ticker
                AND new._extracted_date = old.extracted_date
            WHERE old.id IS NULL
        """)

        print("Dados transformados na camada Bronze.")
