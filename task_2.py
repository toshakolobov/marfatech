import logging
import random
import sqlite3
import sys
from collections import Counter
from pathlib import Path

from common import AddressAggregate

DB_PATH = 'task_1.db'
RANDOM_COUNTER = 100000


def get_rnd_by_weight(weights_map: dict[str, int]) -> str:
    rnd = random.randint(1, sum(weights_map.values()))
    for k, w in sorted(weights_map.items(), key=lambda x: x[1], reverse=True):
        rnd -= w
        if rnd <= 0:
            return k


log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.addHandler(logging.StreamHandler(sys.stdout))

if not Path(DB_PATH).exists():
    log.info(f'Can\'t find {DB_PATH}')
    exit(1)

log.info(f'Database {DB_PATH} has been found')
conn = sqlite3.connect(DB_PATH)
curs = conn.cursor()
query = """
    SELECT * FROM aggregates
"""
curs.execute(query)
aggregates = {item[0]: AddressAggregate(int(item[1]), int(item[2])) for item in curs.fetchall()}
conn.close()

log.info('Init data:')
for k, v in aggregates.items():
    print(f'{k}: {v.sum}')
print('-' * 50)
weights_map = {k: v.sum for k, v in aggregates.items()}

log.info(f'Will execute function for {RANDOM_COUNTER} times')
result = Counter(get_rnd_by_weight(weights_map) for _ in range(100000))
for k, v in result.items():
    print(f'{k}: {v} times')
