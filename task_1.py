import json
import logging
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import requests

from common import AddressAggregate

# constants
API_ADDRESS = 'https://api.etherscan.io/api'
API_TOKEN = 'SKUR3W7T9DE8HSZVREG5AYRF21EE7XFAVK'
ACCOUNT_ADDRESS = '0x690b9a9e9aa1c9db991c7721a92d351db4fac990'
DB_PATH = 'task_1.db'
LOG_PATH = 'task_1.log'


def get_last_block_by_ts(ts: datetime) -> Optional[int]:
    """
    returns the closest block number which was finalized before defined timestamp
    """
    params = {
        'module': 'block',
        'action': 'getblocknobytime',
        'timestamp': int(ts.timestamp()),
        'closest': 'before',
        'apikey': API_TOKEN,
    }
    response = requests.get(API_ADDRESS, params=params)
    if response.status_code == 200:
        json_content: dict = json.loads(response.content)
        return int(json_content['result'])


def get_address_value_from_tx(tx: dict) -> Tuple[str, int, int]:
    if tx['from'] != ACCOUNT_ADDRESS:
        return tx['from'], 0, int(tx['value'])
    else:
        return tx['to'], int(tx['value']), 0


Path(LOG_PATH).unlink(missing_ok=True)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.addHandler(logging.StreamHandler(sys.stdout))
log.addHandler(logging.FileHandler(LOG_PATH))

# defining init values
last_block_txs = []
last_handled_block = 0
aggregates = {}
if Path(DB_PATH).exists():
    log.info(f'Database {DB_PATH} has been found')
    conn = sqlite3.connect(DB_PATH)
    curs = conn.cursor()

    # last txs
    query = """
        SELECT * from last_block_txs
    """
    curs.execute(query)
    last_block_txs = list(curs.fetchall())
    last_handled_block = last_block_txs[-1][-1]

    # existing aggregates
    query = """
        SELECT * FROM aggregates
    """
    curs.execute(query)
    aggregates = {item[0]: AddressAggregate(int(item[1]), int(item[2])) for item in curs.fetchall()}
    conn.close()
else:
    log.info(f'Creating database {DB_PATH}...')
    conn = sqlite3.connect(DB_PATH)
    curs = conn.cursor()
    query = """
        CREATE TABLE aggregates (
            address VARCHAR(42) PRIMARY KEY,
            in_amnt VARCHAR(36),
            out_amnt VARCHAR(36),
            sum_amnt VARCHAR(36)
        );
        CREATE TABLE last_block_txs (
            hash VARCHAR(66),
            from_addr VARCHAR(42),
            to_addr VARCHAR(42),
            amnt VARCHAR(36),
            block_number BIGINT
        );
    """
    curs.executescript(query)
    conn.commit()
    conn.close()
    log.info(f'Successfully created database {DB_PATH}')

# execution
stop_flg = False
start_block = last_handled_block
end_block = get_last_block_by_ts(datetime.utcnow())

conn = sqlite3.connect(DB_PATH)
curs = conn.cursor()

while not stop_flg:
    log.info(f'Fetching data for {start_block=}, {end_block=}')
    params = {
        'module': 'account',
        'action': 'txlistinternal',
        'address': ACCOUNT_ADDRESS,
        'startblock': start_block,
        'endblock': end_block,
        'sort': 'asc',
        'apikey': API_TOKEN
    }
    response = requests.get(API_ADDRESS, params=params)
    response.raise_for_status()
    json_content: dict = json.loads(response.content)
    result: list[dict] = json_content['result']

    last_tx_block = int(result[-1]['blockNumber'])  # last block number in fetched data
    for item in result:
        current_block = int(item["blockNumber"])
        if last_block_txs and current_block != int(last_block_txs[-1][-1]):
            last_block_txs = []
        tx_data = (
            item['hash'], item['from'], item['to'], item['value'], current_block,
        )
        if tx_data not in last_block_txs:
            address, in_, out = get_address_value_from_tx(item)
            if address not in aggregates.keys():
                aggregates[address] = AddressAggregate()
            aggregates[address].in_ += in_
            aggregates[address].out += out
            if current_block == last_tx_block:
                last_block_txs.append(tx_data)
            log.info(
                'hash: {}, from: {}, to: {}, value: {}, block: {}'.format(*tx_data)
            )
        else:
            log.info(
                '(skipped) hash: {}, from: {}, to: {}, value: {}, block: {}'.format(*tx_data)
            )

    if aggregates:
        log.info('Updating info in database')
        params = tuple((k, str(v.in_), str(v.out), str(v.sum)) for k, v in aggregates.items())

        # elegant solution, but, unfortunately, sqlite can't work with so big numbers
        # query = f"""
        #     INSERT INTO aggregates (address, in_amnt, out_amnt, sum_amnt)
        #     VALUES (?, ?, ?, ?)
        #     ON CONFLICT (address) DO UPDATE SET
        #         in_amnt = in_amnt + ?,
        #         out_amnt = out_amnt + ?,
        #         sum_amnt = sum_amnt + ?
        #
        # """
        # curs.executemany(query, params)

        curs.execute('DELETE FROM aggregates;')
        query = """
            INSERT INTO aggregates
            VALUES (?, ?, ?, ?)
        """
        curs.executemany(query, params)

        curs.execute('DELETE FROM last_block_txs;')
        query = """
            INSERT INTO last_block_txs
            VALUES (?, ?, ?, ?, ?)
        """
        curs.executemany(query, last_block_txs)
        conn.commit()
        log.info('Successfully updated database info')

    if last_tx_block != start_block:
        start_block = last_tx_block
    else:
        stop_flg = True
    log.info('-' * 200)
