# apt-get install valgrind
valgrind --leak-check=full --suppressions=valgrind-python.supp python3.9 -E -tt -m pytest --asyncio-mode=strict tests/test_mssql.py > checked.txt 2>&1
