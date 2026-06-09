from abc import ABC, abstractmethod
from typing import List, Optional

from alerta.forwarding.models import ForwardJob


class ForwardingBackend(ABC):

	@abstractmethod
	def publish(self, job: ForwardJob) -> str:
		raise NotImplementedError

	@abstractmethod
	def fetch(self, batch_size: int = 1, block_ms: int = 1000) -> List[ForwardJob]:
		raise NotImplementedError

	@abstractmethod
	def ack(self, job: ForwardJob) -> None:
		raise NotImplementedError

	@abstractmethod
	def delete(self, job: ForwardJob) -> None:
		raise NotImplementedError

	@abstractmethod
	def retry(self, job: ForwardJob, error: str) -> None:
		raise NotImplementedError

	@abstractmethod
	def dead_letter(self, job: ForwardJob, error: str) -> None:
		raise NotImplementedError

	@abstractmethod
	def seen(self, key: str, ttl_seconds: int) -> bool:
		"""Return True if key is seen for the first time (new), else False."""
		raise NotImplementedError

	def close(self) -> Optional[None]:
		return None
