"""Minimal client for the Pseudogram mock API.

Only two operations are used at runtime:

* POST /v1/dm/send  (rate-limited)  -> send one DM
* GET  /v1/dm/{id}  (not rate-limited) -> reconcile delivery status

Every network failure is converted into a sentinel status code (0) so callers
never have to worry about exceptions from httpx.
"""

import httpx

from . import config


class PseudoGramClient:
    def __init__(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=config.PSEUDOGRAM_BASE_URL,
            timeout=config.HTTP_TIMEOUT,
            headers={"X-API-Key": config.API_KEY},
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def send_dm(
        self,
        recipient_user_id: str,
        message: str,
        comment_id: str,
        idempotency_key: str,
    ) -> tuple[int, dict, dict]:
        """Send a DM. Returns (status_code, body, headers).

        status_code 0 means a transport error (timeout / connection failure),
        which callers treat as retryable.
        """
        payload = {
            "recipient_user_id": recipient_user_id,
            "message": message,
            "comment_id": comment_id,
        }
        headers = {"Idempotency-Key": idempotency_key}
        try:
            resp = await self._client.post("/v1/dm/send", json=payload, headers=headers)
            body = self._parse_json(resp)
            return resp.status_code, body, dict(resp.headers)
        except httpx.HTTPError:
            return 0, {}, {}

    async def get_dm(self, dm_id: str) -> tuple[int, dict]:
        """Reconcile a DM's delivery status. Returns (status_code, body)."""
        try:
            resp = await self._client.get(f"/v1/dm/{dm_id}")
            body = self._parse_json(resp)
            return resp.status_code, body
        except httpx.HTTPError:
            return 0, {}

    @staticmethod
    def _parse_json(resp) -> dict:
        """Parse a response body as JSON, never raising on malformed content.

        A body that is not valid JSON is returned as an empty object so callers
        classify it as malformed/unrecognized and apply their bounded-retry
        policy instead of crashing the worker loop.
        """
        if not resp.content:
            return {}
        try:
            return resp.json()
        except ValueError:
            return {}
