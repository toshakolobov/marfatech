task_1.py - первое задание.
При первом запуске создаётся sqlite3 база данных, в которой будут находиться 2 таблицы: aggregates и last_block_txs.
aggregates - целевая таблица, которая необходима по условию задания.
last_block_txs - вспомогательная таблица, в которую записываются учтённые транзакции последнего зачитанного блока.
С помощью этой вспомогательной таблицы скрипт понимает, с какого блока и с какой транзакции ему начать, если его запустить повторно (любое количество раз).

Для меня было полной неожиданностью то, что две транзакции внутри одного блока с разными отправителями/получателями/суммами могут иметь ОДИНАКОВЫЕ хеш-суммы.
Если бы не этот факт, скрипт был бы намного проще.

task_2.py - второе задание.
Запускается только после выполнения task_1.py (и, соответственно, после создания sqlite3 базы данных).
