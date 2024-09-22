from src.utils.db_utils import DuckDBConnection


class SilverTransformer:
    def __init__(self, db_connection: 'DuckDBConnection', config: dict) -> None:
        self.db_connection = db_connection
        self.config = config


    def create_tables(self) -> None:
        """Cria as tabelas necessárias no esquema Bronze."""
        self.db_connection.execute("""
            CREATE SCHEMA IF NOT EXISTS silver;
        """)

        self.db_connection.execute("""
            CREATE TABLE IF NOT EXISTS silver.dim_tempo AS
            SELECT
                id,
                data,
                EXTRACT(YEAR FROM data) AS ano,
                EXTRACT(MONTH FROM data) AS mes,
                EXTRACT(DAY FROM data) AS dia,
                EXTRACT(WEEK FROM data) AS semana,
                EXTRACT(QUARTER FROM data) AS trimestre,
                IF(EXTRACT(MONTH FROM data) <= 6, 1, 2) AS semestre
            FROM bronze.tempo;
        """)

        self.db_connection.execute("""
            CREATE OR REPLACE TABLE silver.dim_acoes (
                id INTEGER PRIMARY KEY,
                ticker VARCHAR,
                name VARCHAR,
                logo VARCHAR,
                sector VARCHAR,
                type VARCHAR,
            );
        """)

        self.db_connection.execute("""
            CREATE OR REPLACE TABLE silver.fato_indicadores (
                id INTEGER PRIMARY KEY,
                tempo_id INTEGER,
                acao_id INTEGER,
                ticker VARCHAR,
                cotacao FLOAT,
                p_vp FLOAT,
                dividend_yield FLOAT,
                ev_ebit FLOAT,
                roic FLOAT,
                p_l FLOAT,
                liquidez_2_meses FLOAT,
                cres_rec_5a FLOAT
            );
        """)

        self.db_connection.execute("""
            CREATE OR REPLACE TABLE silver.fato_oportunidades (
                id INTEGER PRIMARY KEY,
                tempo_id INTEGER,
                acao_id INTEGER,
                ticker VARCHAR,
                cotacao FLOAT,
                p_vp FLOAT,
                dividend_yield FLOAT,
                ev_ebit FLOAT,
                roic FLOAT,
                p_l FLOAT
            );
        """)


    def transform(self) -> None:
        self.db_connection.execute(f"""
            INSERT INTO silver.dim_tempo
            SELECT
                new.id,
                new.data,
                EXTRACT(YEAR FROM new.data) AS ano,
                EXTRACT(MONTH FROM new.data) AS mes,
                EXTRACT(DAY FROM new.data) AS dia,
                EXTRACT(WEEK FROM new.data) AS semana,
                EXTRACT(QUARTER FROM new.data) AS trimestre,
                IF(EXTRACT(MONTH FROM new.data) <= 6, 1, 2) AS semestre
            FROM bronze.tempo AS new
            LEFT JOIN silver.dim_tempo AS old
                ON new.id = old.id
            WHERE old.id IS NULL
        """)

        self.db_connection.execute(f"""
            INSERT INTO silver.dim_acoes
            SELECT
                id,
                ticker,
                name,
                logo,
                sector,
                type
            FROM bronze.brapi_quote_list
        """)

        self.db_connection.execute(f"""
            INSERT INTO silver.fato_indicadores
            SELECT 
                fd.id,
                tempo.id AS tempo_id,
                brapi.id AS acao_id,
                fd.ticker,
                COALESCE(fd.cotacao, brapi.close) AS cotacao,
                fd.p_vp,
                fd.dividend_yield,
                fd.ev_ebit,
                fd.roic,
                fd.p_l,
                fd.liquidez_2_meses,
                fd.cres_rec_5a
            FROM bronze.fundamentus_resultado AS fd
            LEFT JOIN bronze.tempo AS tempo
                ON fd.extracted_date = tempo.data
            LEFT JOIN bronze.brapi_quote_list AS brapi
                ON fd.ticker = brapi.ticker
                AND fd.extracted_date = brapi.extracted_date
        """)

        self.db_connection.execute(f"""
            INSERT INTO silver.fato_oportunidades
            SELECT 
                id,
                tempo_id,
                acao_id,
                ticker,
                cotacao,
                p_vp,
                dividend_yield,
                ev_ebit,
                roic,
                p_l
            FROM silver.fato_indicadores
            WHERE 
                liquidez_2_meses > 100000
                AND cotacao > 0
                AND ev_ebit > 0
                AND p_vp < 1
                AND roic > 0.1
                AND p_l > 0
                AND cres_rec_5a > 0
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY ticker, cotacao, p_vp, dividend_yield, ev_ebit, roic, p_l
                ORDER BY ticker, cotacao, p_vp, dividend_yield, ev_ebit, roic, p_l
            ) = 1
        """)

        print("Dados transformados na camada Silver.")