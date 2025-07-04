import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import luigi

# Set scheduler host (assuming luigid is the container name)
luigi.configuration.get_config().set('scheduler', 'host', 'luigid')
luigi.configuration.get_config().set('scheduler', 'port', '8082')  # optional, 8082 is default

from pipelines.pipeline import StartLoad
from datetime import date

if __name__ == "__main__":
    luigi.build([StartLoad(run_date=date.today())], local_scheduler=False)
