from pathlib import Path

from src.utils.db_utils import DuckDBConnection

class DataLoader:
    def __init__(self, db_connection: 'DuckDBConnection', config: dict) -> None:
        """
        Inicializa o carregador de dados.

        :param db_connection: Conexão com o banco de dados DuckDB.
        :param config: Dicionário de configuração contendo caminhos e outras informações.
        """
        self.db_connection = db_connection
        self.config = config

    def load_data(self) -> None:
        """Carrega dados transformados e salva no formato Parquet na camada Gold."""
        silver_path = Path(self.config['paths']['silver'])
        gold_path = Path(self.config['paths']['gold'])

        # Carregar dados transformados e criar as tabelas
        self.db_connection.sql(f"""
            CREATE TABLE fact_acoes AS
            SELECT * FROM read_parquet('{silver_path}/transformed_acoes.parquet');
        """)

        self.db_connection.sql(f"""
            CREATE TABLE fact_transacoes AS
            SELECT * FROM read_parquet('{silver_path}/transformed_transacoes.parquet');
        """)

        # Salvar as tabelas finais em Parquet
        self.db_connection.save_parquet('fact_acoes', gold_path / 'fact_acoes.parquet')
        self.db_connection.save_parquet('fact_transacoes', gold_path / 'fact_transacoes.parquet')

        print("Dados carregados e salvos na camada Gold.")
