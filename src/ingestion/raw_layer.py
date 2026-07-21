"""
Etapa 1 - Landing Zone
======================
Baixa os arquivos parquet ORIGINAIS (sem nenhuma transformação) publicados
pela NYC TLC para yellow taxis e os grava na landing zone, particionados
por ano/mês (padrão Hive: year=YYYY/month=MM).

Este script é a única etapa do pipeline que fala com a internet. É
intencionalmente simples e desacoplado do Spark: sua única responsabilidade
é trazer o dado bruto para dentro do Data Lake de forma idempotente e
auditável (guardamos o arquivo exatamente como a fonte publicou).

Uso:
    python -m src.ingestion.raw_layer
"""

import logging
import sys
import urllib.request
import urllib.error
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))
from src.config import LANDING_PATH, TLC_BASE_URL, YEAR, MONTHS  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def download_month(year: int, month: int, dest_root: str = LANDING_PATH) -> Path:
    """Baixa o arquivo de um mês específico, se ainda não existir localmente."""
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"{TLC_BASE_URL}/{filename}"

    partition_dir = Path(dest_root) / f"year={year}" / f"month={month:02d}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    dest_file = partition_dir / filename

    if dest_file.exists():
        logger.info("Já existe, pulando download: %s", dest_file)
        return dest_file

    logger.info("Baixando %s -> %s", url, dest_file)
    try:
        urllib.request.urlretrieve(url, dest_file)
    except urllib.error.URLError as exc:
        logger.error("Falha ao baixar %s: %s", url, exc)
        raise

    size_mb = dest_file.stat().st_size / (1024 * 1024)
    logger.info("OK: %s (%.1f MB)", dest_file.name, size_mb)
    return dest_file


def run(year: int = YEAR, months: list = None) -> list:
    months = months or MONTHS
    downloaded = []
    for month in months:
        downloaded.append(download_month(year, month))
    logger.info("Ingestão da landing zone concluída: %d arquivo(s).", len(downloaded))
    return downloaded


if __name__ == "__main__":
    run()
