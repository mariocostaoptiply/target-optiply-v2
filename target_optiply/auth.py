import base64
import backoff
import json
import logging
import os
import requests
from datetime import datetime
from typing import Any, Dict, Optional

from target_hotglue.auth import Authenticator


class OptiplyAuthenticator(Authenticator):
    """API Authenticator for Optiply OAuth 2.0 password flow."""

    def __init__(self, target, state: Dict[str, Any] = {}, auth_endpoint: Optional[str] = None) -> None:
        super().__init__(target, state)  # sets self._config, self._target, self._config_file_path, self.logger
        self._auth_endpoint = auth_endpoint or os.environ.get(
            "optiply_dashboard_url", "https://dashboard.acceptance.optiply.com/api"
        ) + "/auth/oauth/token"

    @property
    def auth_headers(self) -> dict:
        if not self.is_token_valid():
            self.update_access_token()
        return {"Authorization": f"Bearer {self._config.get('access_token')}"}

    @property
    def oauth_request_body(self) -> dict:
        return {
            "grant_type": "password",
            "username": self._config["username"],
            "password": self._config["password"],
            "client_id": self._config["client_id"],
            "client_secret": self._config["client_secret"],
        }

    def is_token_valid(self) -> bool:
        access_token = self._config.get("access_token")
        if not access_token:
            return False
        expires_in = self._config.get("expires_in")
        if not expires_in:
            return True  # no expiry info but token exists — assume valid, 401 handler will refresh if needed
        return not ((int(expires_in) - round(datetime.utcnow().timestamp())) < 120)

    @backoff.on_exception(backoff.expo, Exception, max_tries=3)
    def update_access_token(self) -> None:
        client_id = self._config["client_id"]
        client_secret = self._config["client_secret"]
        basic_auth = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {basic_auth}",
        }
        self.logger.info(f"OAuth request - endpoint: {self._auth_endpoint}")
        token_response = requests.post(self._auth_endpoint, data=self.oauth_request_body, headers=headers)

        try:
            if token_response.json().get("error_description") == "Rate limit exceeded: access_token not expired":
                return
        except Exception:
            raise Exception(f"Failed converting response to JSON: {token_response.text}")

        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            self.state.update({"auth_error_response": token_response.json()})
            raise RuntimeError(f"Failed OAuth login, response was '{token_response.json()}'. {ex}")

        token_json = token_response.json()
        self.logger.info(f"Latest refresh token: {token_json['refresh_token']}")
        self._config["access_token"] = token_json["access_token"]
        self._config["refresh_token"] = token_json["refresh_token"]
        self._config["expires_in"] = int(token_json["expires_in"]) + round(datetime.utcnow().timestamp())

        with open(self._config_file_path, "w") as outfile:
            json.dump(self._config, outfile, indent=4)

    def handle_401_response(self) -> None:
        self.logger.info("Received 401 Unauthorized response, refreshing token...")
        self.update_access_token()
        self.logger.info("Token refreshed after 401 response")
