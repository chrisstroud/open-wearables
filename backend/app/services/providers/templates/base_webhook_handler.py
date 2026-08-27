"""Abstract base class for provider webhook handlers.

This module defines the ``BaseWebhookHandler`` template that every provider
must implement to participate in the unified webhook pipeline.

Design goals
------------
* **Single responsibility** – Each provider subclass owns *only* its own
  verification logic, payload schema, and dispatch routing.
* **Centralised signature helpers** – HMAC-SHA256 and plain-token verification
  are provided as static helpers so providers never have to re-implement
  cryptographic primitives (addresses issue #712).
* **Composable with BaseProviderStrategy** – The handler is wired in via
  ``BaseProviderStrategy.webhooks`` and called by the unified router
  (``/providers/{provider}/webhooks``) through the concrete ``handle()``
  and ``handle_challenge()`` methods.

Delivery-mode awareness
-----------------------
Different providers deliver data in fundamentally different ways:

* **Push (full payload)** – Garmin sends the complete data payload inside the
  webhook body.  ``dispatch()`` can save data immediately.
* **Notify-only (pull-after-push)** – Oura, Strava, Fitbit and others send a
  lightweight notification (user_id + event_type) and expect the consumer to
  fetch the actual data via REST.  ``dispatch()`` should schedule a Celery task
  or call the provider's pull-API directly.
* **Backfill-triggered** – Garmin also supports an async backfill flow where an
  explicit API call initiates a data export that is later delivered via webhook.

Providers indicate which modes they support through
``BaseProviderStrategy.capabilities`` (see ``ProviderCapabilities``).
"""

import hashlib
import hmac
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException, Request
from sqlalchemy import or_

from app.database import DbSession
from app.models import User, UserConnection
from app.services.provider_identity_authority import acquire_provider_identity_value_locks
from app.utils.structured_logging import log_structured

_MAX_WEBHOOK_IDENTITIES = 1_000
_MAX_WEBHOOK_IDENTITY_LENGTH = 255


@dataclass(frozen=True)
class WebhookIngressAuthority:
    """Provider identities whose current account authority is locked for dispatch."""

    provider_user_ids: frozenset[str]
    provider_usernames: frozenset[str]


class BaseWebhookHandler(ABC):
    """Abstract template for provider-specific webhook handlers.

    Subclass this in each provider's package (e.g.
    ``app/services/providers/oura/webhook_handler.py``) and wire it into the
    provider strategy via ``self.webhooks = OuraWebhookHandler()``.

    Required overrides
    ------------------
    * ``verify_signature`` – cryptographic/token-based request verification.
    * ``parse_payload`` – deserialise raw bytes into a typed Pydantic model.
    * ``dispatch`` – route the parsed payload to the appropriate service method.
    * ``supported_event_types`` – declare which event types are handled.

    Optional overrides
    ------------------
    * ``handle_challenge`` – GET-based subscription verification (Strava
      ``hub.challenge``, Oura ``verification_token``).  Defaults to 501.
    * ``handle`` – the full pipeline orchestrator; override only when the
      standard *verify → parse → dispatch* sequence is insufficient.
    """

    #: Top-level webhook-payload key holding the provider-side user id.
    #: Override extract_user_id() instead when it isn't a flat field (e.g. Garmin).
    user_id_field: str | None = None

    def __init__(self, provider_name: str) -> None:
        self.provider_name = provider_name
        self.logger = logging.getLogger(self.__class__.__name__)

    def extract_user_id(self, payload: dict[str, Any]) -> str | None:
        """Best-effort provider-side user id from a raw webhook payload (for log correlation)."""
        if not self.user_id_field:
            return None
        value = payload.get(self.user_id_field)
        return str(value) if value is not None else None

    @staticmethod
    def _bounded_webhook_identities(values: set[str]) -> frozenset[str]:
        """Normalize a bounded set of stable provider identities, dropping unsafe values."""
        if len(values) > _MAX_WEBHOOK_IDENTITIES:
            return frozenset()
        normalized = {
            value.strip() for value in values if value.strip() and len(value.strip()) <= _MAX_WEBHOOK_IDENTITY_LENGTH
        }
        return frozenset(normalized)

    def authorize_webhook_ingress(
        self,
        db: DbSession,
        *,
        provider_user_ids: set[str] | None = None,
        provider_usernames: set[str] | None = None,
    ) -> WebhookIngressAuthority:
        """Lock and authorize exact active legacy-provider identities before side effects.

        The initial lookup discovers candidate account rows without taking locks. User
        rows are then locked in deterministic order, followed by a fresh locked
        connection lookup. This serializes raw storage and enqueueing with source reset:
        once reset fences an account, a provider callback can only be acknowledged and
        dropped. ``apple-mobile-v2-only`` remains closed to every cloud-provider ingress
        even after the paired Apple installation makes the account active again, while
        ``multi-source`` explicitly permits current cloud connections alongside Apple v2.
        """
        requested_user_ids = self._bounded_webhook_identities(provider_user_ids or set())
        requested_usernames = self._bounded_webhook_identities(provider_usernames or set())
        if not requested_user_ids and not requested_usernames:
            return WebhookIngressAuthority(frozenset(), frozenset())
        acquire_provider_identity_value_locks(
            db,
            ((self.provider_name, identity) for identity in sorted(requested_user_ids.union(requested_usernames))),
        )

        identity_filters = []
        if requested_user_ids:
            identity_filters.append(UserConnection.provider_user_id.in_(requested_user_ids))
        if requested_usernames:
            identity_filters.append(UserConnection.provider_username.in_(requested_usernames))

        candidate_user_ids = {
            row[0]
            for row in db.query(UserConnection.user_id)
            .filter(
                UserConnection.provider == self.provider_name,
                UserConnection.status == "active",
                or_(*identity_filters),
            )
            .distinct()
            .all()
        }
        if not candidate_user_ids:
            return WebhookIngressAuthority(frozenset(), frozenset())

        locked_users = (
            db.query(User)
            .filter(User.id.in_(candidate_user_ids))
            .order_by(User.id)
            .populate_existing()
            .with_for_update()
            .all()
        )
        permitted_user_ids = {
            user.id
            for user in locked_users
            if user.health_write_state == "active" and user.health_source_policy in {"legacy-mixed", "multi-source"}
        }
        if not permitted_user_ids:
            return WebhookIngressAuthority(frozenset(), frozenset())

        current_connections = (
            db.query(UserConnection)
            .filter(
                UserConnection.user_id.in_(candidate_user_ids),
                UserConnection.provider == self.provider_name,
                UserConnection.status == "active",
                or_(*identity_filters),
            )
            .order_by(UserConnection.id)
            .populate_existing()
            .with_for_update()
            .all()
        )

        def authorized_values(attribute: str, requested: frozenset[str]) -> frozenset[str]:
            owners_by_value: dict[str, set[Any]] = {}
            for connection in current_connections:
                raw_value = getattr(connection, attribute)
                if raw_value is None:
                    continue
                value = str(raw_value)
                if value in requested:
                    owners_by_value.setdefault(value, set()).add(connection.user_id)
            return frozenset(
                value
                for value, owner_ids in owners_by_value.items()
                if bool(owner_ids) and owner_ids.issubset(permitted_user_ids)
            )

        return WebhookIngressAuthority(
            provider_user_ids=authorized_values("provider_user_id", requested_user_ids),
            provider_usernames=authorized_values("provider_username", requested_usernames),
        )

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    def verify_signature(self, request: Request, body: bytes) -> bool:
        """Verify the authenticity of an incoming webhook request.

        Implementations should use the appropriate scheme for their provider:

        * **HMAC-SHA256** – Oura (``x-oura-signature``), Fitbit
          (``X-Hub-Signature-256``), Suunto
        * **Plain token comparison** – Strava (``hub.verify_token`` query
          param), some Polar endpoints
        * **Shared-secret header** – Garmin (``Authorization`` header token)
        * **Asymmetric / webhook secret** – future providers

        Use the shared helpers ``_verify_hmac_sha256`` and ``_verify_token``
        to avoid re-implementing cryptographic primitives.

        Args:
            request: The incoming FastAPI ``Request`` (headers, query params).
            body: Raw request body bytes (already read by the route dependency).

        Returns:
            ``True`` if verification passes, ``False`` otherwise.
        """

    @abstractmethod
    def parse_payload(self, body: bytes) -> Any:
        """Deserialise the raw webhook body into a provider-specific schema.

        Typically this validates and parses ``body`` into a Pydantic model
        defined under ``app/schemas/providers/{provider}/``.

        Args:
            body: Raw request body bytes.

        Returns:
            A validated payload object.  The concrete type is provider-specific
            and is passed as-is to ``dispatch()``.

        Raises:
            ``HTTPException(400)`` on malformed JSON or schema validation errors.
        """

    @abstractmethod
    def dispatch(self, db: DbSession, payload: Any) -> dict[str, Any]:
        """Route the parsed payload to the appropriate handler.

        This is where the business logic lives.  Implementations should inspect
        the event/data type on ``payload`` and call the relevant service method
        (e.g. ``oura_handler._dispatch_data_type``).

        **Delivery-mode contract:**

        * *Push (full payload)*: save data directly inside this method.
        * *Notify-only*: schedule a Celery task or call the pull API to fetch
          the actual data, then return a result summary.

        Args:
            db: Active database session.
            payload: The validated object returned by ``parse_payload()``.

        Returns:
            A ``dict`` summarising the processing outcome (counts, IDs, …).
            The unified router returns this dict as the HTTP response body.
        """

    @abstractmethod
    def supported_event_types(self) -> list[str]:
        """Return the event type strings this handler can process.

        Used for documentation, observability, and pre-dispatch validation.
        Event type strings should match the values sent by the provider
        (e.g. ``"activity_create"``, ``"daily_sleep"``, ``"PING"``).

        Example::

            return ["activity_create", "activity_update", "activity_delete"]
        """

    # ------------------------------------------------------------------
    # Async processing (webhook_stream providers only)
    # ------------------------------------------------------------------

    def process_payload(self, db: DbSession, payload: Any, trace_id: str) -> dict[str, Any]:
        """Process a previously-enqueued webhook payload.

        Called by the ``process_webhook_push`` Celery task with its own DB
        session. Override in ``webhook_stream`` providers (Garmin, Suunto) where
        ``dispatch()`` enqueues work and this method does the actual processing.

        ``webhook_ping`` providers do not need to override this — they complete
        all processing inside ``dispatch()`` itself.
        """
        raise NotImplementedError(
            f"Provider '{self.provider_name}' must implement process_payload() to use the unified webhook_push_task."
        )

    # ------------------------------------------------------------------
    # Concrete pipeline orchestration
    # ------------------------------------------------------------------

    def handle(self, request: Request, body: bytes, db: DbSession) -> dict[str, Any]:
        """Orchestrate the full webhook handling pipeline.

        Executes: ``verify_signature`` → ``parse_payload`` → ``dispatch``.

        Override this only when the standard sequence is insufficient (e.g.
        when Garmin mixes PING and PUSH in the same endpoint and the routing
        decision must happen before payload parsing).

        Args:
            request: The incoming request.
            body: Raw request body bytes.
            db: Active database session.

        Returns:
            Result ``dict`` from ``dispatch()``.

        Raises:
            ``HTTPException(401)`` if ``verify_signature`` returns ``False``.
            ``HTTPException(400)`` if ``parse_payload`` raises a validation error.
        """
        if not self.verify_signature(request, body):
            log_structured(
                self.logger,
                "warning",
                "Webhook signature verification failed",
                provider=self.provider_name,
                path=request.url.path,
                client_host=request.client.host if request.client else None,
            )
            raise HTTPException(status_code=401, detail="Invalid webhook signature")

        payload = self.parse_payload(body)
        return self.dispatch(db, payload)

    def handle_challenge(self, request: Request) -> dict[str, Any]:
        """Handle GET-based subscription verification challenges.

        Override this in providers that use a GET challenge/response handshake
        to verify webhook subscriptions (Strava ``hub.challenge``, Oura
        ``verification_token``).

        Default implementation raises ``501 Not Implemented`` so that the
        unified router fails loudly for providers that have not wired up
        challenge handling yet.

        Args:
            request: The incoming GET request (query params carry challenge data).

        Returns:
            Provider-specific challenge response dict.

        Raises:
            ``HTTPException(501)`` by default.
        """
        raise HTTPException(
            status_code=501,
            detail=f"Provider '{self.provider_name}' does not support webhook subscription verification via GET.",
        )

    # ------------------------------------------------------------------
    # Shared signature verification helpers (issue #712)
    # ------------------------------------------------------------------

    @staticmethod
    def _verify_hmac_sha256(
        secret: str,
        body: bytes,
        provided_signature: str,
        *,
        prefix: bytes = b"",
        case_insensitive: bool = False,
    ) -> bool:
        """Constant-time HMAC-SHA256 signature verification.

        Args:
            secret: The shared secret key (UTF-8 string).
            body: The message bytes to authenticate.
            provided_signature: The hex digest received in the request header.
            prefix: Optional bytes prepended to ``body`` before signing.
                    Oura uses ``timestamp_bytes + body``; most providers use
                    ``body`` alone.
            case_insensitive: When ``True``, both digests are upper-cased
                              before comparison (required by Oura).

        Returns:
            ``True`` if the computed digest matches ``provided_signature``.

        Example (Oura)::

            ts = request.headers.get("x-oura-timestamp", "")
            sig = request.headers.get("x-oura-signature", "")
            ok = BaseWebhookHandler._verify_hmac_sha256(
                secret, body, sig,
                prefix=ts.encode(),
                case_insensitive=True,
            )
        """
        mac = hmac.new(secret.encode(), prefix + body, hashlib.sha256)
        expected = mac.hexdigest()
        if case_insensitive:
            return hmac.compare_digest(expected.upper(), provided_signature.upper())
        return hmac.compare_digest(expected, provided_signature)

    @staticmethod
    def _verify_token(expected_token: str, provided_token: str) -> bool:
        """Constant-time plain-token comparison.

        Suitable for providers that use a shared-secret token without
        any request-body signing (e.g. Strava ``hub.verify_token``,
        Garmin ``Authorization`` header).

        Args:
            expected_token: Token value from settings (ground truth).
            provided_token: Token value received in the request.

        Returns:
            ``True`` if the tokens match.
        """
        return hmac.compare_digest(expected_token, provided_token)
