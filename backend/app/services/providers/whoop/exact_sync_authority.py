from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from threading import Event
from uuid import UUID


@dataclass(frozen=True)
class ExactWhoopSyncAuthority:
    dispatch_id: UUID
    user_id: UUID
    connection_id: UUID
    authorization_generation: int
    lease_token: UUID
    lease_lost: Event = field(default_factory=Event, compare=False, repr=False)


_exact_whoop_sync_authority: ContextVar[ExactWhoopSyncAuthority | None] = ContextVar(
    "exact_whoop_sync_authority",
    default=None,
)


def current_exact_whoop_sync_authority() -> ExactWhoopSyncAuthority | None:
    return _exact_whoop_sync_authority.get()


@contextmanager
def scoped_exact_whoop_sync_authority(authority: ExactWhoopSyncAuthority) -> Iterator[None]:
    token = _exact_whoop_sync_authority.set(authority)
    try:
        yield
    finally:
        _exact_whoop_sync_authority.reset(token)
