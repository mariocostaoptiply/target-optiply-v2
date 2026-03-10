"""Optiply target sink class, which handles writing streams."""

from __future__ import annotations

import backoff
import logging
import os
import requests
from typing import Any, Dict, List, Optional

from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from target_hotglue.client import HotglueSink
from singer_sdk.plugin_base import PluginBase

from target_optiply.auth import OptiplyAuthenticator

logger = logging.getLogger(__name__)


class OptiplySink(HotglueSink):
    """Optiply target sink class."""

    base_url = os.environ.get("optiply_base_url", "https://api.acceptance.optiply.com/v1")
    http_headers = {
        "Content-Type": "application/vnd.api+json",
        "Accept": "application/vnd.api+json",
    }

    @property
    def authenticator(self) -> OptiplyAuthenticator:
        return OptiplyAuthenticator(self._target, {})

    def url(self, endpoint: str = "") -> str:
        params = {}
        if "account_id" in self.config:
            params["accountId"] = self.config["account_id"]
        if "coupling_id" in self.config:
            params["couplingId"] = self.config["coupling_id"]

        url = f"{self.base_url}/{endpoint}"
        if params:
            query_string = "&".join(f"{k}={v}" for k, v in params.items())
            url = f"{url}?{query_string}"
        return url

    def _get_error_message(self, response_text: str, status_code: int, url: str) -> str:
        """Get a meaningful error message from response text."""
        if not response_text or response_text.strip() in ["", "null", "None"]:
            return f"No error details provided (status {status_code})"

        try:
            import json
            error_data = json.loads(response_text)
            if isinstance(error_data, dict):
                if "errors" in error_data and isinstance(error_data["errors"], list):
                    error_messages = []
                    for error in error_data["errors"]:
                        if isinstance(error, dict):
                            if "meta" in error and "message" in error["meta"]:
                                error_messages.append(error["meta"]["message"])
                            elif "detail" in error:
                                error_messages.append(error["detail"])
                            elif "message" in error:
                                error_messages.append(error["message"])
                    if error_messages:
                        return f"API Error: {'; '.join(error_messages)}"
                elif "message" in error_data:
                    return f"API Error: {error_data['message']}"
                elif "error" in error_data:
                    return f"API Error: {error_data['error']}"
        except (Exception):
            pass

        if len(response_text.strip()) > 0:
            return f"API Error: {response_text}"
        return f"No error details provided (status {status_code})"

    def validate_response(self, response: requests.Response) -> None:
        if response.status_code >= 500:
            error_msg = self._get_error_message(response.text, response.status_code, response.url)
            raise RetriableAPIError(f"Server error ({response.status_code}): {error_msg}")
        elif response.status_code == 404:
            error_msg = self._get_error_message(response.text, response.status_code, response.url)
            logger.warning(f"Resource not found (404): {response.url} - {error_msg}")
            return
        elif response.status_code == 401:
            error_msg = self._get_error_message(response.text, response.status_code, response.url)
            raise FatalAPIError(f"Authentication failed after token refresh ({response.status_code}): {error_msg}")
        elif response.status_code >= 400:
            error_msg = self._get_error_message(response.text, response.status_code, response.url)
            raise FatalAPIError(f"Client error ({response.status_code}): {error_msg}")

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ReadTimeout),
        max_tries=5,
        factor=2,
    )
    def _request(self, http_method, endpoint, params=None, request_data=None, headers=None) -> requests.Response:
        url = self.url(endpoint)
        headers = self.default_headers

        response = requests.request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data,
        )

        if response.status_code == 401:
            logger.info("Received 401 error, attempting to refresh token and retry")
            try:
                self.authenticator.handle_401_response()
                headers = self.default_headers
                response = requests.request(
                    method=http_method,
                    url=url,
                    params=params,
                    headers=headers,
                    json=request_data,
                )
                if response.status_code == 401:
                    raise FatalAPIError(f"Authentication failed after token refresh: {response.text}")
            except Exception as e:
                logger.error(f"Failed to refresh token and retry: {str(e)}")
                raise

        self.validate_response(response)
        return response

    def request_api(self, http_method: str, endpoint: str = None, params: dict = {}, request_data: dict = None, headers: dict = {}) -> requests.Response:
        @backoff.on_exception(backoff.expo,
                              (requests.exceptions.RequestException, ConnectionResetError),
                              max_tries=3, max_time=30)
        def _make_request():
            url = self.url(endpoint)
            request_headers = self.default_headers.copy()
            if headers:
                request_headers.update(headers)

            self.logger.info(f"Request: {http_method} /{endpoint} | Payload: {request_data}")

            response = requests.request(
                method=http_method,
                url=url,
                json=request_data,
                headers=request_headers,
                timeout=30,
            )

            if response.status_code >= 400:
                error_msg = self._get_error_message(response.text, response.status_code, url)
                self.logger.error(f"Request Status: {response.status_code} | Error: {error_msg}")
                if response.status_code >= 500:
                    self.logger.error(f"Request URL: {url} | Payload: {request_data}")
            else:
                self.logger.info(f"Request Status: {response.status_code}")

            return response

        response = _make_request()

        if response.status_code == 401:
            logger.info("Received 401 error in request_api, attempting to refresh token and retry")
            try:
                self.authenticator.handle_401_response()
                response = _make_request()
                if response.status_code == 401:
                    raise FatalAPIError(f"Authentication failed after token refresh: {response.text}")
            except Exception as e:
                logger.error(f"Failed to refresh token and retry: {str(e)}")
                raise

        return response
