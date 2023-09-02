import datetime
from typing import Tuple, List, Union, Optional


class Cursor:
    """
    Cursor class
    """
    pass

    def __iter__(self) -> Cursor:
        pass

    def __next__(self) -> Cursor:
        pass

    def __await__(self) -> Cursor:
        pass

    def __enter__(self) -> Cursor:
        pass

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass

    async def execute(
        self,
        query: str,
        params: Optional[
            Tuple[Union[None, int, bool, float, str, datetime.datetime, datetime.date, datetime.time]]
        ] = None,
        timeout: int = 0
    ) -> Cursor:
        """
        Asynchronous execute the sql query
        :param query: query-string
        :param params: parameters to pass to the sql query
        :param timeout: query timeout: 0 - infinite, 2147483647 - max. Default 0
        :return: Cursor
        """
        pass

    def fetchall(self) -> List[dict]:
        """
        Getting query results
        :return: results
        """
        pass

    def close(self) -> None:
        """
        Close this cursor
        :return: None
        """
        pass


class Connection:
    """
    Connection class
    """
    def __iter__(self) -> Connection:
        pass

    def __next__(self) -> Connection:
        pass

    def __await__(self) -> Connection:
        pass

    async def __aenter__(self) -> Connection:
        pass

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> Connection:
        pass

    def cursor(self) -> Cursor:
        """
        Create a cursor on this connection
        :return: Cursor
        """
        pass

    async def close(self) -> Connection:
        """
        Asynchronous close this connection
        :return: Connection
        """
        pass


async def connect(dsn: str, timeout: int = 0) -> Connection:
    """
    Asynchronous create a connection to a server
    :param dsn: connection string
    :param timeout: login timeout: 0 - infinite, 2147483647 - max. Default 0
    :return: Connection
    """
    pass


_rate: float = 1.0
