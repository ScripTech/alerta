import threading
import time

from flask import Request, current_app
from mohawk import Receiver


def get_credentials(key_id: str):
    credentials_map = {
        creds['key']: dict(
            id=creds['key'],  # access_key
            key=creds['secret'],  # secret_key
            algorithm=creds.get('algorithm', 'sha256')
        ) for creds in current_app.config['HMAC_AUTH_CREDENTIALS']}

    if key_id in credentials_map:
        return credentials_map[key_id]
    else:
        raise LookupError('Unknown sender')


class HmacAuth:

    _nonce_lock = threading.Lock()
    _nonce_seen = {}

    @staticmethod
    def seen_nonce(sender_id: str, nonce: str, ts: str) -> bool:
        """Return True when a nonce has already been processed for this sender."""
        key = f'{sender_id}:{nonce}:{ts}'
        now = time.time()
        ttl = max(int(current_app.config.get('HMAC_NONCE_TTL', 300)), 1)

        with HmacAuth._nonce_lock:
            # Best effort cleanup to keep the in-memory nonce map bounded.
            for old_key, expiry in list(HmacAuth._nonce_seen.items()):
                if expiry <= now:
                    HmacAuth._nonce_seen.pop(old_key, None)

            if key in HmacAuth._nonce_seen:
                return True

            HmacAuth._nonce_seen[key] = now + ttl
            return False

    @staticmethod
    def authenticate(r: Request):
        return Receiver(
            get_credentials,
            r.headers.get('Authorization'),
            url=r.url,
            method=r.method,
            content=r.data,
            content_type=r.content_type,
            seen_nonce=HmacAuth.seen_nonce,
            timestamp_skew_in_seconds=300
        )
