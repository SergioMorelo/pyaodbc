# python -m pytest --asyncio-mode=strict tests\test_mssql.py
# python -m pytest --asyncio-mode=strict tests/test_mssql.py
# python -m pytest --asyncio-mode=strict --capture=sys tests\test_mssql.py

import asyncio
import datetime
import os

import pyaodbc
import pytest
import pytest_asyncio


@pytest_asyncio.fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope='module')
async def connection():
    dsn = "DRIVER={ODBC Driver 18 for SQL Server};" \
          f"SERVER={os.environ.get('SERVER_ADDRESS')};" \
          f"DATABASE={os.environ.get('DB_NAME')};" \
          f"UID={os.environ.get('USER_NAME')};" \
          f"PWD={os.environ.get('USER_PASSWORD')};" \
          "TrustServerCertificate=yes;"

    async with pyaodbc.connect(dsn, 3) as conn:
        yield conn


@pytest.fixture(scope='function')
def cursor(connection):
    with connection.cursor() as cur:
        yield cur


@pytest.mark.parametrize(
    ('sql_type', 'python_type'), [
        ('convert(bigint, -9223372036854775808)', -9_223_372_036_854_775_808),
        ('convert(bigint, -9223372036854775807)', -9_223_372_036_854_775_807),
        ('convert(int, -2147483648)', -2147483648),
        ('convert(int, 2147483647)', 2147483647),
        ('convert(smallint, -32768)', -32768),
        ('convert(smallint, 32767)', 32767),
        ('convert(tinyint, 0)', 0),
        ('convert(tinyint, 255)', 255),
        ('convert(decimal(5,2), 123)', 123.00),
        ('convert(decimal(5,2), -123)', -123.00),
        ('convert(numeric(10,5), 12345.12)', 12345.12000),
        ('convert(numeric(10,5), -12345.12)', -12345.12000),
        ('convert(bit, 1)', True),
        ('convert(bit, 0)', False),
        ("convert(float, -1.79E+308)", -1.79E+308),
        ("convert(float, -2.23E-308)", -2.23E-308),
        ("convert(float, 0)", 0.0),
        ("convert(float, 2.23E-308)", 2.23E-308),
        ("convert(float, 1.79E+308)", 1.79E+308),
        # ("convert(real, '-3.40E+38')", -3.40E+38),  # mssql data type does not provide sufficient precision
        # ("convert(real, -1.18E-38)", -1.18E-38),  # mssql data type does not provide sufficient precision
        ("convert(real, 0)", 0.0),
        # ("convert(real, 1.18E-38)", 1.18E-38),  # mssql data type does not provide sufficient precision
        # ("convert(real, 3.40E+38)", 3.40E+38),  # mssql data type does not provide sufficient precision
        ("convert(date, '0001-01-01')", datetime.date(1, 1, 1)),
        ("convert(date, '9999-12-31')", datetime.date(9999, 12, 31)),
        ("convert(datetime, '1753-01-01 00:00:00')", datetime.datetime(1753, 1, 1, 0, 0, 0)),
        ("convert(datetime, '99991231 23:59:59.997')", datetime.datetime(9999, 12, 31, 23, 59, 59, 997000)),
        ("convert(datetime2, '00010101 00:00:00')", datetime.datetime(1, 1, 1, 0, 0, 0, 0)),
        ("convert(datetime2, '99991231 23:59:59.999999')", datetime.datetime(9999, 12, 31, 23, 59, 59, 999999)),
        ("convert(time, '00:00:00.0000000')", datetime.time(0, 0, 0, 0)),
        ("convert(time, '23:59:59.9999999')", datetime.time(23, 59, 59, 0)),
        ("convert(datetimeoffset(3), '00010101 00:00:00')", '0001-01-01 00:00:00.000 +00:00'),
        ("convert(datetimeoffset(3), '99991231 23:59:59.9999999')", '9999-12-31 23:59:59.999 +00:00'),
        ("convert(smalldatetime, '1990-01-01 00:00:32')", datetime.datetime(1990, 1, 1, 0, 1, 0, 0)),
        ("convert(smalldatetime, '2079-06-06  23:59:03')", datetime.datetime(2079, 6, 6, 23, 59, 0, 0)),
        ("convert(char(4), 'test')", 'test'),
        ("convert(char, 'test')", 'test' + ' ' * 26),
        ("convert(varchar, 'test')", 'test'),
        ("convert(text, 'test')", 'test'),
        ("convert(ntext, N'тест')", 'тест'),
        ("convert(nchar(4), N'тест')", 'тест'),
        ("convert(nvarchar, N'тест')", 'тест'),
        ("convert(nvarchar, N'😊')", '😊'),
        ("convert(ntext, N'тест')", 'тест'),
        ("'default'", 'default'),
        ("N'по-умолчанию'", 'по-умолчанию')
    ]
)
@pytest.mark.asyncio
async def test_get_data(cursor, sql_type, python_type):
    query = f'select TestField = {sql_type}'
    await cursor.execute(query)
    result = cursor.fetchall()
    assert result[0]['TestField'] == python_type


@pytest.mark.parametrize(
    ('sql_type', 'python_type'), [
        ('convert(bigint, -9223372036854775808)', -9_223_372_036_854_775_808),
        ('convert(bigint, -9223372036854775807)', -9_223_372_036_854_775_807),
        ('convert(int, -2147483648)', -2147483648),
        ('convert(int, 2147483647)', 2147483647),
        ('convert(smallint, -32768)', -32768),
        ('convert(smallint, 32767)', 32767),
        ('convert(tinyint, 0)', 0),
        ('convert(tinyint, 255)', 255),

        # decimal and numeric put as string
        ('convert(decimal(5,2), 123)', '123.00'),
        ('convert(decimal(5,2), -123)', '-123.00'),
        ('convert(numeric(10,5), 12345.12)', '12345.12000'),
        ('convert(numeric(10,5), -12345.12)', '-12345.12000'),

        ('convert(bit, 1)', True),
        ('convert(bit, 0)', False),

        ("convert(float, -1.79E+308)", -1.79E+308),
        ("convert(float, -2.23E-308)", -2.23E-308),
        ("convert(float, 0)", 0.0),
        ("convert(float, 2.23E-308)", 2.23E-308),
        ("convert(float, 1.79E+308)", 1.79E+308),

        # real put as string
        # test it in the mssql:
        # select 1 where convert(real, -3.40E+38) = '-3.40E+38'
        # select 1 where convert(real, -3.40E+38) = -3.40E+38
        ("convert(real, -3.40E+38)", '-3.40E+38'),  # ~~
        ("convert(real, -1.18E-38)", '-1.18E-38'),  # ~~
        ("convert(real, 0)", '0.0'),
        ("convert(real, 1.18E-38)", '1.18E-38'),  # ~~
        ("convert(real, 3.40E+38)", '3.40E+38'),  # ~~

        ("convert(date, '0001-01-01')", datetime.date(1, 1, 1)),
        ("convert(date, '9999-12-31')", datetime.date(9999, 12, 31)),
        ("convert(datetime, '1753-01-01 00:00:00')", datetime.datetime(1753, 1, 1, 0, 0, 0)),

        # from CPython to SQL in the presence of milliseconds the date will always be converted to datetime2
        ("convert(datetime, '99991231 23:59:59.000')", datetime.datetime(9999, 12, 31, 23, 59, 59)),
        ("convert(datetime2, '00010101 00:00:00')", datetime.datetime(1, 1, 1, 0, 0, 0, 0)),
        ("convert(datetime2, '99991231 23:59:59.999999')", datetime.datetime(9999, 12, 31, 23, 59, 59, 999999)),
        ("convert(time, '00:00:00.0000000')", datetime.time(0, 0, 0, 0)),

        # in ODBC there are no milliseconds for time structure in time structure
        ("convert(time, '23:59:59.000')", datetime.time(23, 59, 59, 0)),
        ("convert(datetimeoffset(3), '00010101 00:00:00')", '0001-01-01 00:00:00.000 +00:00'),
        ("convert(datetimeoffset(3), '99991231 23:59:59.9999999')", '9999-12-31 23:59:59.999 +00:00'),
        ("convert(smalldatetime, '1990-01-01 00:00:32')", datetime.datetime(1990, 1, 1, 0, 1, 0, 0)),
        ("convert(smalldatetime, '2079-06-06  23:59:03')", datetime.datetime(2079, 6, 6, 23, 59, 0, 0)),
    ]
)
@pytest.mark.asyncio
async def test_input_data(cursor, sql_type, python_type):
    query = f"select TestField = 'Test' where {sql_type} = ?"
    params = (python_type, )
    await cursor.execute(query, params)
    result = cursor.fetchall()
    assert len(result) == 1


@pytest.fixture(scope='function')
def some_coro():
    async def coro(tm, phrase):
        return await asyncio.sleep(tm, phrase)
    return coro


@pytest.fixture(scope='function')
def some_query(cursor):
    async def coro(phrase):
        query = f"""
            waitfor delay '00:00:02'
            select TestField = '{phrase}'
        """
        await cursor.execute(query)
        result = cursor.fetchall()
        return result[0].get('TestField')
    return coro


@pytest.mark.asyncio
async def test_nonblocking_execute(some_coro, some_query):
    results = []
    phrases = ['i should be done first', 'i should be done second', 'i should be done third']
    for c in asyncio.as_completed([some_query(phrases[1]), some_coro(3, phrases[2]), some_coro(1, phrases[0])]):
        result = await c
        results.append(result)
    assert results == phrases
