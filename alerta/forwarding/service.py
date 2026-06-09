import logging
from typing import TYPE_CHECKING, Any, Dict, Optional

from alerta.exceptions import ForwardingQueue
from alerta.forwarding.backends.base import ForwardingBackend
from alerta.forwarding.backends.memory import MemoryForwardingBackend
from alerta.forwarding.backends.redis import RedisForwardingBackend
from alerta.forwarding.models import ForwardJob
from alerta.utils.client import Client


if TYPE_CHECKING:
    from alerta.models.alert import Alert  # noqa

LOG = logging.getLogger('alerta.plugins.forwarder')

class ForwarderQueueService:
    _backend: Optional[ForwardingBackend] = None
    _backend_name: Optional[str] = None

    @classmethod
    def _get_backend(cls, config: Optional[Dict[str, Any]] = None) -> ForwardingBackend:
        cfg = config or {}
        name = str(cfg.get('FORWARDING_BACKEND', 'memory')).lower()

        if cls._backend and cls._backend_name == name:
            return cls._backend

        if name == 'redis':
            LOG.info('Using Redis forwarding backend')
            cls._backend = RedisForwardingBackend(
                url=cfg.get('FORWARD_REDIS_URL', 'redis://localhost:6379/0'),
                stream=cfg.get('FORWARD_REDIS_STREAM', 'alerta:forward'),
                group=cfg.get('FORWARD_REDIS_GROUP', 'alerta-forwarders'),
                consumer=cfg.get('FORWARD_REDIS_CONSUMER', 'alerta-worker'),
                dlq_stream=cfg.get('FORWARD_REDIS_DLQ_STREAM', 'alerta:forward:dlq'),
            )
        else:
            LOG.info('Using in-memory forwarding backend')
            cls._backend = MemoryForwardingBackend()

        cls._backend_name = name
        return cls._backend

    @staticmethod
    def _is_in_xloop(remote: str, x_loop: str) -> bool:
        return bool(remote and x_loop and remote in x_loop)

    @classmethod
    def build_alert_job(
        cls,
        alert: 'Alert',
        remote: str,
        auth: Dict[str, Any],
        headers: Dict[str, str],
        config: Optional[Dict[str, Any]] = None,
    ) -> ForwardJob:
        cfg = config or {}
        body = alert.get_body(history=False)
        body['id'] = alert.last_receive_id

        key = f'alert:{remote}:{body.get("id") or alert.id}'
        job = ForwardJob(
            kind='alert',
            remote=remote,
            auth=auth,
            payload=body,
            headers=headers,
            idempotency_key=key,
            max_attempts=int(cfg.get('FORWARD_MAX_RETRIES', 3)),
            retry_delay=int(cfg.get('FORWARD_RETRY_BACKOFF', 2)),
            dlq_enabled=bool(cfg.get('FORWARD_DLQ_ENABLED', True)),
        )
        LOG.debug(f'Building alert forwarding job with idempotency job: {job}')
        return job

    @classmethod
    def build_action_job(
        cls,
        alert_id: str,
        action: str,
        text: str,
        remote: str,
        auth: Dict[str, Any],
        headers: Dict[str, str],
        config: Optional[Dict[str, Any]] = None,
    ) -> ForwardJob:
        cfg = config or {}
        key = f'action:{remote}:{alert_id}:{action}:{text}'
        job = ForwardJob(
            kind='action',
            remote=remote,
            auth=auth,
            payload={'alert_id': alert_id, 'action': action, 'text': text},
            headers=headers,
            idempotency_key=key,
            max_attempts=int(cfg.get('FORWARD_MAX_RETRIES', 3)),
            retry_delay=int(cfg.get('FORWARD_RETRY_BACKOFF', 2)),
            dlq_enabled=bool(cfg.get('FORWARD_DLQ_ENABLED', True)),
        )
        LOG.debug(f'Building action forwarding job with idempotency job: {job}')
        return job

    @classmethod
    def build_delete_job(
        cls,
        alert_id: str,
        remote: str,
        auth: Dict[str, Any],
        headers: Dict[str, str],
        config: Optional[Dict[str, Any]] = None,
    ) -> ForwardJob:
        cfg = config or {}
        key = f'delete:{remote}:{alert_id}'
        return ForwardJob(
            kind='delete',
            remote=remote,
            auth=auth,
            payload={'alert_id': alert_id},
            headers=headers,
            idempotency_key=key,
            max_attempts=int(cfg.get('FORWARD_MAX_RETRIES', 3)),
            retry_delay=int(cfg.get('FORWARD_RETRY_BACKOFF', 2)),
            dlq_enabled=bool(cfg.get('FORWARD_DLQ_ENABLED', True)),
        )

    @staticmethod
    def publish(job: ForwardJob, config: Optional[Dict[str, Any]] = None) -> str:
        backend = ForwarderQueueService._get_backend(config)
        ttl = int((config or {}).get('FORWARD_IDEMPOTENCY_TTL', 600))
        if not backend.seen(job.idempotency_key, ttl):
            LOG.debug(f'Forward queue duplicate dropped: {job.idempotency_key}')
            return 'duplicate'
        return backend.publish(job)

    @staticmethod
    def consume(config: Optional[Dict[str, Any]] = None):
        backend = ForwarderQueueService._get_backend(config)
        cfg = config or {}
        batch = int(cfg.get('FORWARD_BATCH_SIZE', 50))
        return backend.fetch(batch_size=batch)

    @staticmethod
    def execute(job: ForwardJob) -> None:
        LOG.debug(f'Executing forwarding job: {job}')
        client = Client(endpoint=job.remote, headers=job.headers, **job.auth)
        if job.kind == 'alert':
            client.send_alert(**job.payload)
            return
        if job.kind == 'action':
            client.action(job.payload['alert_id'], job.payload['action'], job.payload.get('text', ''))
            return
        if job.kind == 'delete':
            client.delete_alert(job.payload['alert_id'])
            return
        raise ForwardingQueue(f'Unknown forwarding job kind: {job.kind}')

