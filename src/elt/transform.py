from src.elt.transformations.bronze_transformations import BronzeTransformer
from src.elt.transformations.silver_transformations import SilverTransformer
from src.elt.transformations.gold_transformations import GoldTransformer
from src.utils.db_utils import DuckDBConnection

class DataTransformer:
    def __init__(self, db_connection: 'DuckDBConnection', config: dict) -> None:
        """
        Inicializa o transformador de dados.

        :param db_connection: Conexão com o banco de dados DuckDB.
        :param config: Dicionário de configuração contendo caminhos e outras informações.
        """
        self.db_connection = db_connection
        self.config = config

    def transform_data(self) -> None:
        """Orquestra as transformações de dados nas camadas Bronze, Silver e Gold."""
        bronze_transformer = BronzeTransformer(self.db_connection, self.config)
        bronze_transformer.create_tables()
        bronze_transformer.transform()

        silver_transformer = SilverTransformer(self.db_connection, self.config)
        silver_transformer.create_tables()
        silver_transformer.transform()

        # gold_transformer = GoldTransformer(self.db_connection, self.config)
        # gold_transformer.transform()

        print("Transformações de dados concluídas.")