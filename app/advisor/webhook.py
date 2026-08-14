from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from app.advisor.approval import ApprovalService
from app.advisor.whatsapp import WhatsAppConfig


def build_webhook_handler(approval: ApprovalService, config: WhatsAppConfig):
    """Build a private Meta webhook handler without exposing advisor secrets."""

    class WhatsAppWebhookHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            query = parse_qs(urlparse(self.path).query)
            mode = query.get("hub.mode", [""])[0]
            token = query.get("hub.verify_token", [""])[0]
            challenge = query.get("hub.challenge", [""])[0]
            if mode != "subscribe" or not config.webhook_verify_token or token != config.webhook_verify_token:
                self._json({"ok": False, "error": "webhook_verification_failed"}, HTTPStatus.FORBIDDEN)
                return
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(challenge.encode("utf-8"))))
            self.end_headers()
            self.wfile.write(challenge.encode("utf-8"))

        def do_POST(self) -> None:  # noqa: N802
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                length = 0
            if length <= 0 or length > 1_000_000:
                self._json({"ok": False, "error": "invalid_body_size"}, HTTPStatus.BAD_REQUEST)
                return
            raw_body = self.rfile.read(length)
            # Each request owns its SQLite connection. ThreadingHTTPServer may
            # dispatch this handler on a thread different from construction.
            request_store = type(approval.store)(
                approval.store.db_path,
                integrity_key=approval.store.integrity_key,
                confirmation_key=approval.store.confirmation_key,
            )
            try:
                request_approval = ApprovalService(
                    request_store,
                    allowed_numbers=tuple(approval.allowed_numbers),
                    app_secret=approval.app_secret,
                )
                result = request_approval.handle_webhook(
                    raw_body=raw_body,
                    signature_header=self.headers.get("X-Hub-Signature-256", ""),
                )
            finally:
                request_store.close()
            if result.status == "retry":
                status = HTTPStatus.SERVICE_UNAVAILABLE
            elif result.status == "rejected" and result.reason == "webhook_signature_invalid":
                status = HTTPStatus.FORBIDDEN
            else:
                status = HTTPStatus.OK if result.status in {"confirmed", "rejected", "duplicate", "expired"} else HTTPStatus.BAD_REQUEST
            self._json({"status": result.status, "proposal_id": result.proposal_id, "reason": result.reason}, status)

        def _json(self, payload: dict, status: HTTPStatus) -> None:
            body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return

    return WhatsAppWebhookHandler


def run_webhook_server(
    approval: ApprovalService,
    config: WhatsAppConfig,
    *,
    host: str = "127.0.0.1",
    port: int = 8787,
) -> None:
    server = ThreadingHTTPServer((host, int(port)), build_webhook_handler(approval, config))
    server.serve_forever()
