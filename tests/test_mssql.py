# python -m pytest --asyncio-mode=strict tests/test_mssql.py
# python -m pytest --asyncio-mode=strict --capture=sys tests\test_mssql.py

import asyncio
import datetime
import os

import pyaodbc
import pytest
import pytest_asyncio


if os.environ.get('WSL'):
    pyaodbc._rate = 0.1

DSN = "DRIVER={ODBC Driver 18 for SQL Server};" \
          f"SERVER={os.environ.get('SERVER_ADDRESS')};" \
          f"DATABASE={os.environ.get('DB_NAME')};" \
          f"UID={os.environ.get('USER_NAME')};" \
          f"PWD={{{os.environ.get('USER_PASSWORD')}}};" \
          "TrustServerCertificate=yes;"

types_matching_getting_data = [
    ('convert(bigint, -9223372036854775808)', -9_223_372_036_854_775_808),
    ('convert(bigint, -9223372036854775807)', -9_223_372_036_854_775_807),
    ('convert(bigint, null)', None),
    ('convert(int, -2147483648)', -2147483648),
    ('convert(int, 2147483647)', 2147483647),
    ('convert(int, null)', None),
    ('convert(smallint, -32768)', -32768),
    ('convert(smallint, 32767)', 32767),
    ('convert(smallint, null)', None),
    ('convert(tinyint, 0)', 0),
    ('convert(tinyint, 255)', 255),
    ('convert(tinyint, null)', None),
    ('convert(decimal(5,2), 123)', 123.00),
    ('convert(decimal(5,2), -123)', -123.00),
    ('convert(decimal(5,2), null)', None),
    ('convert(numeric(10,5), 12345.12)', 12345.12000),
    ('convert(numeric(10,5), -12345.12)', -12345.12000),
    ('convert(numeric(10,5), null)', None),
    ('convert(bit, 1)', True),
    ('convert(bit, 0)', False),
    ('convert(bit, null)', None),
    ("convert(float, -1.79E+308)", -1.79E+308),
    ("convert(float, -2.23E-308)", -2.23E-308),
    ("convert(float, 0)", 0.0),
    ("convert(float, 2.23E-308)", 2.23E-308),
    ("convert(float, 1.79E+308)", 1.79E+308),
    ("convert(float, null)", None),
    # ("convert(real, '-3.40E+38')", -3.40E+38),  # the mssql data type doesn't provide sufficient precision
    # ("convert(real, -1.18E-38)", -1.18E-38),  # the mssql data type doesn't provide sufficient precision
    ("convert(real, 0)", 0.0),
    ("convert(real, null)", None),
    # ("convert(real, 1.18E-38)", 1.18E-38),  # the mssql data type doesn't provide sufficient precision
    # ("convert(real, 3.40E+38)", 3.40E+38),  # the mssql data type doesn't provide sufficient precision
    ("convert(date, '0001-01-01')", datetime.date(1, 1, 1)),
    ("convert(date, '9999-12-31')", datetime.date(9999, 12, 31)),
    ("convert(date, null)", None),
    ("convert(datetime, '1753-01-01 00:00:00')", datetime.datetime(1753, 1, 1, 0, 0, 0)),
    ("convert(datetime, '99991231 23:59:59.997')", datetime.datetime(9999, 12, 31, 23, 59, 59, 997000)),
    ("convert(datetime, null)", None),
    ("convert(datetime2, '00010101 00:00:00')", datetime.datetime(1, 1, 1, 0, 0, 0, 0)),
    ("convert(datetime2, '99991231 23:59:59.999999')", datetime.datetime(9999, 12, 31, 23, 59, 59, 999999)),
    ("convert(datetime2, null)", None),
    ("convert(time, '00:00:00.0000000')", datetime.time(0, 0, 0, 0)),
    ("convert(time, '23:59:59.9999999')", datetime.time(23, 59, 59, 0)),
    ("convert(time, null)", None),
    ("convert(datetimeoffset(3), '00010101 00:00:00')", '0001-01-01 00:00:00.000 +00:00'),
    ("convert(datetimeoffset(3), '99991231 23:59:59.9999999')", '9999-12-31 23:59:59.999 +00:00'),
    ("convert(datetimeoffset(3), null)", None),
    ("convert(smalldatetime, '1990-01-01 00:00:32')", datetime.datetime(1990, 1, 1, 0, 1, 0, 0)),
    ("convert(smalldatetime, '2079-06-06  23:59:03')", datetime.datetime(2079, 6, 6, 23, 59, 0, 0)),
    ("convert(smalldatetime, null)", None),
    ("convert(char(4), 'test')", 'test'),
    ("convert(char, 'test')", 'test' + ' ' * 26),
    ("convert(char, null)", None),
    ("convert(varchar, 'test')", 'test'),
    ("convert(varchar(max), 'test')", 'test'),
    ("convert(varchar(max), null)", None),
    ("convert(text, 'test')", 'test'),
    ("convert(text, null)", None),
    ("convert(ntext, N'тест')", 'тест'),
    ("convert(ntext, null)", None),
    ("convert(nchar(4), N'тест')", 'тест'),
    ("convert(nchar(4), null)", None),
    ("convert(nvarchar, N'тест')", 'тест'),
    ("convert(nvarchar, null)", None),
    ("convert(nvarchar(max), N'тест')", 'тест'),
    ("convert(nvarchar(max), null)", None),
    ("convert(nvarchar, N'😊')", '😊'),
    ("'default'", 'default'),
    ("N'по-умолчанию'", 'по-умолчанию')
]

types_matching_binding_data = [
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

    # there aren't milliseconds in time structure in ODBC
    ("convert(time, '23:59:59.000')", datetime.time(23, 59, 59, 0)),
    ("convert(datetimeoffset(3), '00010101 00:00:00')", '0001-01-01 00:00:00.000 +00:00'),
    ("convert(datetimeoffset(3), '99991231 23:59:59.9999999')", '9999-12-31 23:59:59.999 +00:00'),
    ("convert(smalldatetime, '1990-01-01 00:00:32')", datetime.datetime(1990, 1, 1, 0, 1, 0, 0)),
    ("convert(smalldatetime, '2079-06-06  23:59:03')", datetime.datetime(2079, 6, 6, 23, 59, 0, 0))
]


@pytest_asyncio.fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope='module')
async def connection():
    async with pyaodbc.connect(DSN, 3) as conn:
        yield conn


@pytest.fixture(scope='function')
def cursor(connection):
    with connection.cursor() as cur:
        yield cur


@pytest.mark.parametrize(
    ('sql_type', 'python_type'), types_matching_getting_data
)
@pytest.mark.asyncio
async def test_get_data(cursor, sql_type, python_type):
    query = f'select TestField = {sql_type}'
    await cursor.execute(query, timeout=5)
    result = cursor.fetchall()
    assert result[0]['TestField'] == python_type


@pytest.mark.parametrize(
    ('sql_type', 'python_type'), types_matching_binding_data
)
@pytest.mark.asyncio
async def test_input_data(cursor, sql_type, python_type):
    query = f"select TestField = 'Test' where {sql_type} = ?"
    params = (python_type, )
    await cursor.execute(query, params, timeout=5)
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
        await cursor.execute(query, timeout=5)
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


@pytest.mark.asyncio
async def test_concurrent_execution():
    async def execute(query, params=None):
        async with pyaodbc.connect(DSN, 3) as conn:
            with conn.cursor() as cur:
                await cur.execute(query, params, timeout=5)
                return cur.fetchall()

    tasks = [execute(f'select TestField = {task[0]}') for task in types_matching_getting_data]
    tasks.extend([
        execute(f"select TestField = 'Test' where {task[0]} = ?", (task[1],)) for task in types_matching_binding_data
    ])

    results = await asyncio.gather(*tasks)
    assert len(results) == len(tasks)


@pytest_asyncio.fixture(scope='function')
async def f_conn():
    conn = await pyaodbc.connect(DSN, 3)
    yield conn
    try:
        await conn.close()
    except:
        pass


@pytest.fixture(scope='function')
def f_cur(f_conn):
    cur = f_conn.cursor()
    yield cur
    try:
        cur.close()
    except:
        pass


@pytest.mark.parametrize(
    ('dsn', 'timeout', 'exc_value'), [
        (DSN, -3, '(PyAODBC_Connect) The value must be nonnegative'),
        (DSN, 2_147_483_648, '(PyAODBC_Connect) The value must be less than 2147483648'),
        (b'123', 1, '(PyAODBC_Connect) The connection string must be an Unicode string'),
    ]
)
@pytest.mark.asyncio
async def test_exception_in_set_connection_timeout(dsn, timeout, exc_value):
    with pytest.raises(AttributeError) as exc_info:
        await pyaodbc.connect(dsn, timeout)
    assert exc_info.value.args[0] == exc_value


@pytest.fixture(scope='function')
def pyaodbc_rate():
    current_rate = pyaodbc._rate
    yield
    pyaodbc._rate = current_rate


@pytest.mark.asyncio
async def test_exception_in_change_pyaodbc_rate(pyaodbc_rate):
    with pytest.raises(ValueError) as exc_info:
        pyaodbc._rate = 'qwerty'
        await pyaodbc.connect(DSN, 1)
    assert exc_info.value.args[0] == '(PyAODBC_Connect) The rate value must be float'


@pytest.mark.asyncio
async def test_exception_neagative_rate_value_in_change_pyaodbc_rate(pyaodbc_rate):
    with pytest.raises(AttributeError) as exc_info:
        pyaodbc._rate = -1.0
        await pyaodbc.connect(DSN, 1)
    assert exc_info.value.args[0] == '(PyAODBC_Connect) The rate value must be nonnegative'


@pytest.mark.asyncio
async def test_exception_is_connection_established(f_conn):
    with pytest.raises(Exception) as exc_info:
        await f_conn
    assert exc_info.value.args[0] == '(Connection_Next) The connection is already established'


@pytest.mark.asyncio
async def test_exception_is_connection_disconnected(f_conn):
    with pytest.raises(Exception) as exc_info:
        await f_conn.close()
        await f_conn
    assert exc_info.value.args[0] == '(Connection_Next) The connection is already disconnected'


@pytest.mark.asyncio
async def test_exception_in_connection_disconnect(f_conn):
    with pytest.raises(Exception) as exc_info:
        await f_conn.close()
        await f_conn.close()
    assert exc_info.value.args[0] == '(disconnect_async) The connection is already disconnected'


@pytest.mark.asyncio
async def test_exception_in_create_cursor_on_close_connection(f_conn):
    with pytest.raises(Exception) as exc_info:
        await f_conn.close()
        f_conn.cursor()
    assert exc_info.value.args[0] == "(Connection_Cursor) The connection isn't established"


@pytest.mark.asyncio
async def test_exception_in_close_cursor_on_close_connection(f_conn):
    with pytest.raises(Exception) as exc_info:
        cur = f_conn.cursor()
        await f_conn.close()
        cur.close()
    assert exc_info.value.args[0] == "(free_cursor) The connection isn't established"


@pytest.mark.asyncio
async def test_exception_in_close_cursor_on_not_opened_cursor(f_cur):
    with pytest.raises(Exception) as exc_info:
        f_cur.close()
        f_cur.close()
    assert exc_info.value.args[0] == "(free_cursor) The cursor isn't opened"


@pytest.mark.asyncio
async def test_exception_in_close_cursor_on_prepare_execute(f_cur):
    with pytest.raises(Exception) as exc_info:
        statement = f_cur.execute("select TestField = 'Test'", timeout=5)
        f_cur.close()
    assert exc_info.value.args[0] == "(free_cursor) Canceling of the executed instruction isn't implemented"
    await statement


@pytest.mark.asyncio
async def test_exception_in_cursor_execute_on_alredy_executed(f_cur):
    with pytest.raises(Exception) as exc_info:
        statement = f_cur.execute("select TestField = 'Test'", timeout=5)
        await statement
        await statement
    assert exc_info.value.args[0] == '(Cursor_Next) The cursor is already executed, You need to take results'


@pytest.mark.asyncio
async def test_exception_expected_coroutine_on_cursor(f_cur):
    with pytest.raises(TypeError) as exc_info:
        await f_cur
    assert exc_info.value.args[0] == '(Cursor_Next) A coroutine was expected'


@pytest.mark.parametrize(
    ('query', 'params', 'count_query_parameters', 'params_length'), [
        ('select TestField = ?', (42, 123), 1, 2),
        ("select TestField = 'Test'", (42, 123), 0, 2),
        ('select TestFieldOne = ?, TestFieldTwo = ?', (42,), 2, 1)
    ]
)
@pytest.mark.asyncio
async def test_exception_in_check_parameters_equality(f_cur, query, params, count_query_parameters, params_length):
    with pytest.raises(TypeError) as exc_info:
        await f_cur.execute(query, params, timeout=5)
    assert exc_info.value.args[0] == f'(check_parameters_equality) The query takes {count_query_parameters} ' \
                                     f'arguments, but {params_length} were given'


@pytest.mark.asyncio
async def test_exception_in_prepare_execute_on_close_connection(f_conn):
    with pytest.raises(Exception) as exc_info:
        cur = f_conn.cursor()
        await f_conn.close()
        cur.execute("select TestField = 'Test'", timeout=5)
    assert exc_info.value.args[0] == "(prepare_execute) The connection isn't established"


@pytest.mark.asyncio
async def test_exception_in_prepare_execute_on_close_cursor(f_cur):
    with pytest.raises(Exception) as exc_info:
        f_cur.close()
        f_cur.execute("select TestField = 'Test'", timeout=5)
    assert exc_info.value.args[0] == "(prepare_execute) The cursor isn't opened"


@pytest.mark.asyncio
async def test_exception_in_prepare_execute_on_runned_cursors(f_conn):
    with pytest.raises(Exception) as exc_info:
        cur1 = f_conn.cursor()
        cur2 = f_conn.cursor()
        await asyncio.gather(
            cur1.execute("select TestField = 'Test'", timeout=5),
            cur2.execute("select TestField = 'Test'", timeout=5)
        )
    assert exc_info.value.args[0] == "(prepare_execute) The number of cursors from one connection can't exceed " \
                                     "max concurrent activities (1)"


@pytest.mark.parametrize(
    'params', [
        ([1, 2, 3],),
        (b'123',),
    ]
)
@pytest.mark.asyncio
async def test_exception_in_checking_params_on_prepare_execute(f_cur, params):
    with pytest.raises(TypeError) as exc_info:
        await f_cur.execute('select TestField = ?', params, timeout=5)
    assert exc_info.value.args[0] == f"(prepare_execute) The parameter type 1 in the query " \
                                     f"isn't supported for passing to SQL"


@pytest.mark.parametrize(
    ('query', 'timeout', 'expected'), [
        ("select TestField = 'Test'", -1, '(Cursor_Execute) The value must be nonnegative'),
        ("select TestField = 'Test'", 2_147_483_648, '(Cursor_Execute) The value must be less than 2147483648'),
        (b'123', 3, '(Cursor_Execute) The query must be an Unicode string'),
    ]
)
@pytest.mark.asyncio
async def test_exception_timeout_on_cursor_execute(f_cur, query, timeout, expected):
    with pytest.raises(AttributeError) as exc_info:
        await f_cur.execute(query, timeout=timeout)
    assert exc_info.value.args[0] == expected


@pytest.mark.asyncio
async def test_exception_in_params_on_cursor_execute(f_cur):
    with pytest.raises(TypeError) as exc_info:
        await f_cur.execute("select TestField = ?", 42, timeout=5)
    assert exc_info.value.args[0] == "(Cursor_Execute) Params must be in a tuple"


@pytest.mark.asyncio
async def test_exception_in_cursor_fetchall_on_closed_connection(f_conn):
    with pytest.raises(Exception) as exc_info:
        cur = f_conn.cursor()
        await cur.execute("select TestField = 'Test'", timeout=5)
        await f_conn.close()
        cur.fetchall()
    assert exc_info.value.args[0] == "(Cursor_Fetchall) The connection isn't established"


@pytest.mark.asyncio
async def test_exception_in_cursor_fetchall_on_closed_cursor(f_cur):
    with pytest.raises(Exception) as exc_info:
        await f_cur.execute("select TestField = 'Test'", timeout=5)
        f_cur.close()
        f_cur.fetchall()
    assert exc_info.value.args[0] == "(Cursor_Fetchall) The cursor isn't opened"


@pytest.mark.asyncio
async def test_exception_in_cursor_fetchall_on_not_executed_cursor(f_cur):
    with pytest.raises(Exception) as exc_info:
        f_cur.fetchall()
    assert exc_info.value.args[0] == "(Cursor_Fetchall) The cursor wasn't executed"
