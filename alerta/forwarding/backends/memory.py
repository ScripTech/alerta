import time
import uuid
from collections import deque
from threading import Lock
from typing import Deque, Dict, List, Tuple

from alerta.forwarding.backends.base import ForwardingBackend
from alerta.forwarding.models import ForwardJob


class MemoryForwardingBackend(ForwardingBackend):
	def __init__(self) -> None:
		self._queue: Deque[ForwardJob] = deque()
		self._inflight: Dict[str, ForwardJob] = {}
		self._dlq: Deque[Tuple[ForwardJob, str]] = deque()
		self._seen: Dict[str, float] = {}
		self._lock = Lock()

	def publish(self, job: ForwardJob) -> str:
		with self._lock:
			if not job.id:
				job.id = str(uuid.uuid4())
			self._queue.append(job)
			return job.id

    # Note: This simple in-memory implementation does not support blocking fetch with timeout. Instead, it will poll the queue until the timeout is reached.
	def fetch(self, batch_size: int = 1, block_ms: int = 1000) -> List[ForwardJob]:
		timeout = time.time() + (block_ms / 1000.0)
		while time.time() < timeout:
			with self._lock:
				if self._queue:
					jobs: List[ForwardJob] = []
					for _ in range(min(batch_size, len(self._queue))):
						job = self._queue.popleft()
						if not job.id:
							job.id = str(uuid.uuid4())
						self._inflight[job.id] = job
						jobs.append(job)
					return jobs
			time.sleep(0.05)
		return []

    # This implementation assumes that the job ID is unique and is used to track inflight jobs. In a real-world scenario, additional error handling and edge case management would be necessary.
	def ack(self, job: ForwardJob) -> None:
		return

	def delete(self, job: ForwardJob) -> None:
		with self._lock:
			if job.id:
				self._inflight.pop(job.id, None)

	def retry(self, job: ForwardJob, error: str) -> None:
		with self._lock:
			if job.id:
				self._inflight.pop(job.id, None)
			self._queue.append(job)

    # In this simple implementation, the dead letter queue is just a deque of tuples containing the job and the error message. In a production system, you might want to persist these to a database or log them for later analysis.
	def dead_letter(self, job: ForwardJob, error: str) -> None:
		with self._lock:
			if job.id:
				self._inflight.pop(job.id, None)
			self._dlq.append((job, error))

	def seen(self, key: str, ttl_seconds: int) -> bool:
		now = time.time()
		with self._lock:
			expiry = self._seen.get(key)
			if expiry and expiry > now:
				return False
			self._seen[key] = now + max(ttl_seconds, 1)
			# Best effort cleanup to keep memory bounded.
			for old_key, old_expiry in list(self._seen.items()):
				if old_expiry <= now:
					self._seen.pop(old_key, None)
			return True
