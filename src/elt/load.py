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
        gold_path = Path(self.config['paths']['gold'])

        self.db_connection.save_parquet('silver.dim_tempo', gold_path / 'dim_tempo.parquet')
        self.db_connection.save_parquet('silver.dim_acoes', gold_path / 'dim_acoes.parquet')

        self.db_connection.save_parquet('silver.fact_oportunidades', gold_path / 'fact_oportunidades.parquet')
        self.db_connection.save_parquet('silver.fact_indicadores', gold_path / 'fact_indicadores.parquet')

        print("Dados carregados e salvos na camada Gold.")
