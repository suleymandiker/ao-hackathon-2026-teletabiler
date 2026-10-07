"""Small HTTP transport for idempotent OpenSearch acquisition requests."""

import requests

from ingestion_layer.opensearch_config import OpenSearchConfig, _host_url


class OpenSearchClientError(RuntimeError):
    """Sanitized transport/response failure; never contains document bodies."""


class OpenSearchClient:
    """Transport only: two bounded passes across hosts, no query or hit logic.

    Injected sessions must be dedicated to this client. Environment-based proxy,
    CA and netrc settings are disabled in favor of the explicit configuration.
    The caller closes injected sessions; this client closes sessions it creates.
    """

    _RETRY_STATUSES = frozenset((429, 500, 502, 503, 504))

    def __init__(self, config: OpenSearchConfig, *, session: requests.Session | None = None):
        self.config = config
        self._hosts = tuple(_host_url(host, config.use_ssl) for host in config.hosts)
        self._owns_session = session is None
        self._session = requests.Session() if session is None else session
        self._session.trust_env = False

    def close(self):
        if self._owns_session:
            self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def post_json(self, path: str, body: dict, *, params=None) -> dict:
        """POST an idempotent read; do not use this retry policy for writes.

        Redirects, TLS failures, invalid requests and malformed JSON fail without
        retry. Raised errors are built outside exception handlers so requests'
        potentially sensitive exception text is not retained as an error chain.
        """
        if (type(path) is not str or not path.startswith("/") or path.startswith("//")
                or any(character in path for character in ("?", "#", "\\"))):
            raise OpenSearchClientError("Invalid request path")
        if type(body) is not dict:
            raise OpenSearchClientError("JSON request must be an object")
        error = "OpenSearch request failed"
        for attempt in range(2 * len(self._hosts)):
            response = None
            retry = False
            try:
                response = self._session.post(
                    self._hosts[attempt % len(self._hosts)] + path,
                    json=body,
                    params=params,
                    auth=(self.config.username, self.config.password),
                    verify=self.config.ca_bundle or self.config.verify_certs,
                    timeout=(self.config.connect_timeout, self.config.request_timeout),
                    allow_redirects=False,
                )
            except requests.exceptions.SSLError:
                error = "OpenSearch TLS failure"
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                error = "OpenSearch connection or timeout failure"
                retry = True
            except (requests.exceptions.RequestException, ValueError, OSError):
                error = "OpenSearch request/configuration failure"

            if response is None:
                if retry:
                    continue
                raise OpenSearchClientError(error)

            try:
                status = response.status_code
                if status in self._RETRY_STATUSES:
                    error = f"OpenSearch transient HTTP failure ({status})"
                    continue
                if not 200 <= status < 300:
                    raise OpenSearchClientError(f"OpenSearch HTTP failure ({status})")
                payload = None
                try:
                    payload = response.json()
                except ValueError:
                    pass
                if type(payload) is not dict:
                    raise OpenSearchClientError("OpenSearch response must be a JSON object")
                return payload
            finally:
                response.close()
        raise OpenSearchClientError(error)
