import yaml
from src.utils.db_utils import DuckDBConnection
from src.elt.extract import DataExtractor
from src.elt.transform import DataTransformer
from src.elt.load import DataLoader
from pathlib import Path

# Carregando a configuração do projeto
config_path = Path('configs/settings.yaml')
with open(config_path, 'r') as file:
    config = yaml.safe_load(file)

# Criando conexão com o banco DuckDB em disco
db_conn = DuckDBConnection(config['paths']['db'])

# Executando o pipeline ELT
extractor = DataExtractor(config)
extractor.extract_fundamentus()
extractor.extract_usuarios_negociacoes()
extractor.extract_brapi()

transformer = DataTransformer(db_conn, config)
transformer.transform_data()

loader = DataLoader(db_conn, config)
loader.load_data()

# Fechando a conexão com o banco
db_conn.close()
