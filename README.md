# Quick Guide
``` python
import asyncio
import os
import pyaodbc


async def example():
    dsn = "DRIVER={ODBC Driver 18 for SQL Server};" \
          f"SERVER={os.environ.get('SERVER_ADDRESS')};" \
          f"DATABASE={os.environ.get('DB_NAME')};" \
          f"UID={os.environ.get('USER_NAME')};" \
          f"PWD={os.environ.get('USER_PASSWORD')};" \
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
