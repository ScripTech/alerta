from typing import Any, Dict, List

from alerta.exceptions import ForwardingQueue
from alerta.forwarding.backends.base import ForwardingBackend
from alerta.forwarding.models import ForwardJob


class RedisForwardingBackend(ForwardingBackend):
	def __init__(
		self,
		url: str,
		stream: str = 'alerta:forward',
		group: str = 'alerta-forwarders',
		consumer: str = 'alerta-worker',
		dlq_stream: str = 'alerta:forward:dlq',
	) -> None:
		try:
			import redis  # type: ignore
		except ImportError as e:
			raise ForwardingQueue('Redis backend requested but redis package is not installed') from e

		self._redis = redis.Redis.from_url(url, decode_responses=True)
		self._stream = stream
		self._group = group
		self._consumer = consumer
		self._dlq_stream = dlq_stream

		try:
			self._redis.xgroup_create(self._stream, self._group, id='0', mkstream=True)
		except Exception as e:
			if 'BUSYGROUP' not in str(e):
				raise ForwardingQueue(f'Error creating Redis stream group: {e}') from e

	def publish(self, job: ForwardJob) -> str:
		payload = {'job': job.to_json()}
		return str(self._redis.xadd(self._stream, payload))

	def fetch(self, batch_size: int = 1, block_ms: int = 1000) -> List[ForwardJob]:
		entries = self._redis.xreadgroup(
			groupname=self._group,
			consumername=self._consumer,
			streams={self._stream: '>'},
			count=batch_size,
			block=block_ms,
		)
		jobs: List[ForwardJob] = []
		for _, messages in entries:
			for stream_id, fields in messages:
				raw = fields.get('job')
				if not raw:
					continue
				job = ForwardJob.from_json(raw)
				job.id = stream_id
				jobs.append(job)
		return jobs

	def ack(self, job: ForwardJob) -> None:
		if not job.id:
			return
		self._redis.xack(self._stream, self._group, job.id)

	def delete(self, job: ForwardJob) -> None:
		if not job.id:
			return
		self._redis.xdel(self._stream, job.id)

	def retry(self, job: ForwardJob, error: str) -> None:
		if job.id:
			self._redis.xack(self._stream, self._group, job.id)
			self._redis.xdel(self._stream, job.id)
		self.publish(job)

	def dead_letter(self, job: ForwardJob, error: str) -> None:
		fields: Dict[str, Any] = {
			'job': job.to_json(),
			'error': error,
		}
		self._redis.xadd(self._dlq_stream, fields)
		if job.id:
			self._redis.xack(self._stream, self._group, job.id)
			self._redis.xdel(self._stream, job.id)

	def seen(self, key: str, ttl_seconds: int) -> bool:
		seen_key = f'alerta:forward:seen:{key}'
		return bool(self._redis.set(seen_key, '1', ex=max(ttl_seconds, 1), nx=True))

	def close(self) -> None:
		try:
			self._redis.close()
		except Exception:
			return
