from pathlib import Path
import sys
import gzip
import shutil
import tempfile
import random
import pandas as pd

# allow importing project modules
PROJECT_ROOT = Path('.').resolve()
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import Config

# Dynamically import the data_loader module without triggering heavy deps
import importlib.util

def load_data_loader_module():
    loader_path = PROJECT_ROOT / 'modules' / 'data_loader.py'
    import types
    if 'pyspark' not in sys.modules:
        sys.modules['pyspark'] = types.ModuleType('pyspark')
    if 'pyspark.sql' not in sys.modules:
        sys.modules['pyspark.sql'] = types.ModuleType('pyspark.sql')
        sys.modules['pyspark.sql'].SparkSession = lambda *a, **k: None
    spec = importlib.util.spec_from_file_location('data_loader_mod', str(loader_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_sample_gz(input_gz_path: Path, out_gz_path: Path, sample_size: int = 2000, random_sampling: bool = True):
    # Read the cleaned gz in streaming mode and produce a sampled gz with same header
    # We'll use pandas in chunks to avoid loading full file
    reader = pd.read_csv(gzip.open(input_gz_path, 'rt', encoding='utf-8'), sep='|', engine='python', chunksize=10000)
    header_written = False
    collected = []
    for chunk in reader:
        if not header_written:
            cols = chunk.columns
            header_written = True
        # sample from chunk
        if random_sampling:
            # sample fractionally but don't exceed sample_size
            sample = chunk.sample(n=min(len(chunk), sample_size if not collected else max(0, sample_size - len(collected))))
        else:
            sample = chunk.head(max(0, sample_size - len(collected)))
        collected.append(sample)
        if sum(len(c) for c in collected) >= sample_size:
            break
    if not collected:
        raise RuntimeError('No rows found in input calendar file')
    df_sample = pd.concat(collected, ignore_index=True)
    # write to gz as pipe-separated same as cleaned
    df_sample.to_csv(out_gz_path, sep='|', index=False, compression='gzip')
    return out_gz_path


if __name__ == '__main__':
    cfg = Config()
    cleaned = list(Path(cfg.CLEANED_DATA_FOLDER).glob('*calendar*.csv*'))
    if not cleaned:
        print('No cleaned calendar files found in', cfg.CLEANED_DATA_FOLDER)
        sys.exit(1)
    input_gz = cleaned[0]
    print('Selected calendar file for sampling:', input_gz)

    sample_size = 2000
    sample_path = Path(cfg.CLEANED_DATA_FOLDER) / f'sample_calendar_{input_gz.stem}_{sample_size}.csv.gz'
    print('Creating sample gz:', sample_path)
    make_sample_gz(input_gz, sample_path, sample_size=sample_size, random_sampling=True)

    # load loader module and run calendar loader on the sample
    mod = load_data_loader_module()
    AirbnbDataLoader = getattr(mod, 'AirbnbDataLoader')
    config = Config()
    loader = AirbnbDataLoader(config)
    conn = loader.db_config.create_connection(database='AirbnbDataWarehouse')
    try:
        loader._load_calendar_data(conn, str(sample_path))
    finally:
        conn.close()
    print('Done')
