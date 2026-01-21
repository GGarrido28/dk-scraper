import logging
from typing import Tuple, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

DEFAULT_RETRIES = 3
DEFAULT_BACKOFF = 1.0
DEFAULT_STATUS_FORCELIST = (429, 500, 502, 503, 504)
DEFAULT_TIMEOUT = (10, 30)  # (connect, read)


class HTTPHandler:
    """HTTP request handler with retry logic and session management."""

    def __init__(
        self,
        retries: int = DEFAULT_RETRIES,
        backoff_factor: float = DEFAULT_BACKOFF,
        status_forcelist: Tuple[int, ...] = DEFAULT_STATUS_FORCELIST,
        timeout: Union[float, Tuple[float, float]] = DEFAULT_TIMEOUT,
    ):
        """
        Initialize the HTTP handler.

        Args:
            retries: Number of retry attempts.
            backoff_factor: Backoff factor between retries.
            status_forcelist: HTTP status codes that trigger a retry.
            timeout: Request timeout (connect, read) or single value for both.
        """
        self.timeout = timeout
        self._session = self._create_session(retries, backoff_factor, status_forcelist)

    def _create_session(
        self,
        retries: int,
        backoff_factor: float,
        status_forcelist: Tuple[int, ...],
    ) -> requests.Session:
        """Create a requests session with retry logic."""
        session = requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def get(self, url: str, **kwargs) -> requests.Response:
        """
        Perform a GET request with retry logic.

        Args:
            url: The URL to request.
            **kwargs: Additional arguments passed to requests.get().

        Returns:
            Response object.

        Raises:
            requests.RequestException: If the request fails after all retries.
        """
        logger.debug(f"Requesting {url}")
        if "timeout" not in kwargs:
            kwargs["timeout"] = self.timeout
        response = self._session.get(url, **kwargs)
        return response

    @property
    def session(self) -> requests.Session:
        """Access the underlying session for advanced use cases."""
        return self._session


def create_session(
    retries: int = DEFAULT_RETRIES,
    backoff_factor: float = DEFAULT_BACKOFF,
    status_forcelist: Tuple[int, ...] = DEFAULT_STATUS_FORCELIST,
) -> requests.Session:
    """
    Create a standalone requests session with retry logic.

    Convenience function for cases where you just need a configured session.

    Args:
        retries: Number of retry attempts.
        backoff_factor: Backoff factor between retries.
        status_forcelist: HTTP status codes that trigger a retry.

    Returns:
        Configured requests session.
    """
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
