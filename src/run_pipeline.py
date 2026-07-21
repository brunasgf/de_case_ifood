"""
Orquestra o pipeline completo: Landing -> Bronze -> Silver.

Uso local:
    python -m src.run_pipeline
"""

import logging

from src.ingestion import bronze_layer
from src.transformation import silver_layer

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def main():
    logger.info("### Etapa 2/3 - Bronze ###")
    bronze_layer.run()

    logger.info("### Etapa 3/3 - Silver ###")
    silver_layer.run()

    logger.info("Pipeline concluído. Tabela de consumo pronta para uso via SQL.")


if __name__ == "__main__":
    main()
