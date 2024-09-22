from src.utils.db_utils import DuckDBConnection


class GoldTransformer:
    def __init__(self, db_connection: 'DuckDBConnection', config: dict) -> None:
        self.db_connection = db_connection
        self.config = config
    

def create_transform_tables(self) -> None:
    """Cria as tabelas necessárias no esquema Bronze."""
    self.db_connection.execute("""
        CREATE SCHEMA IF NOT EXISTS gold;
    """)

    self.db_connection.execute("""
        CREATE OR REPLACE TABLE gold.fato_oportunidades AS
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
            fd.p_l
        FROM bronze.fundamentus_resultado AS fd
        LEFT JOIN bronze.tempo AS tempo
            ON fd.extracted_date = tempo.data
        LEFT JOIN bronze.brapi_quote_list AS brapi
            ON fd.ticker = brapi.ticker
            AND fd.extracted_date = brapi.extracted_date
    """)

    print("Dados transformados na camada Gold.")