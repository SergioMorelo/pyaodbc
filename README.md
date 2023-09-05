# Library for asynchronous connection and execution of queries to the database via ODBC driver
## Software requirements

### Install ODBC Driver for your DBMS
On the example of MS SQL: [link](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16)  
For support the async mode on Windows, the version of the driver must support asynchronous execution  

### Install necessary packages/libraries
**On GNU/Linux:**
- g++>=4:8.3.0-1
- python3-dev=<python_version>
- python3-pip
- unixodbc-dev>=2.3.7

The example for Debian:
``` bash
apt install -y g++ python3-dev python3-pip unixodbc-dev
```

**On Windows:**
- Microsoft Visual C++ compiler version>=14
- The Windows SDK version>=8
- ODBC Lib version>=3.81 (It must be included in SDK)

## Installation PyAODBC
### From pypi.org
```
pip install pyaodbc
```

### From source
``` bash
git clone https://github.com/SergioMorelo/pyaodbc.git
cd pyaodbc
python3 setup.py install
```

## Quick Guide
``` python
import asyncio
import os
import pyaodbc


async def example():
    dsn = "DRIVER={ODBC Driver 18 for SQL Server};" \
          f"SERVER={os.environ.get('SERVER_ADDRESS')};" \
          f"DATABASE={os.environ.get('DB_NAME')};" \
          f"UID={os.environ.get('USER_NAME')};" \
          f"PWD={{{os.environ.get('USER_PASSWORD')}}};" \
          "TrustServerCertificate=yes;"

    async with pyaodbc.connect(dsn) as conn:
        with conn.cursor() as cur:
            query = """
                select ExampleField = 'Hello World'
                where 1 = ?
            """
            await cur.execute(query, (1, ))
            rows = cur.fetchall()
            return rows


if __name__ == '__main__':
    result = asyncio.run(example())
    print(result)

```

### There's the attribute for control CPU Usage (e.g. for Kubernetes, if your CPU value less than 1) and blocking I/O:
``` python
import pyaodbc


pyaodbc._rate = 0.5  # default 1.0
# Than smaller the value, then more operations will be performed by CPU during iterations
# The attribute value will apply to all connections and cursors created with it

async def example():
    dsn = '...'
    async with pyaodbc.connect(dsn) as conn:
        with conn.cursor() as cur:
            ...

```

### Additional information
Additional information on the py-library interface is inside `pyaodbc.pyi`
