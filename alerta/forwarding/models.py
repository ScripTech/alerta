import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class ForwardJob:
	kind: str
	remote: str
	auth: Dict[str, Any]
	payload: Dict[str, Any]
	headers: Dict[str, str]
	idempotency_key: str
	created_at: float = field(default_factory=time.time)
	attempts: int = 0
	max_attempts: int = 3
	retry_delay: int = 2
	dlq_enabled: bool = True
	id: Optional[str] = None

	def to_dict(self) -> Dict[str, Any]:
		return {
			'id': self.id,
			'kind': self.kind,
			'remote': self.remote,
			'auth': self.auth,
			'payload': self.payload,
			'headers': self.headers,
			'idempotency_key': self.idempotency_key,
			'created_at': self.created_at,
			'attempts': self.attempts,
			'max_attempts': self.max_attempts,
			'retry_delay': self.retry_delay,
			'dlq_enabled': self.dlq_enabled,
		}

	@classmethod
	def from_dict(cls, raw: Dict[str, Any]) -> 'ForwardJob':
		return cls(
			id=raw.get('id'),
			kind=raw['kind'],
			remote=raw['remote'],
			auth=raw.get('auth', {}),
			payload=raw.get('payload', {}),
			headers=raw.get('headers', {}),
			idempotency_key=raw['idempotency_key'],
			created_at=float(raw.get('created_at', time.time())),
			attempts=int(raw.get('attempts', 0)),
			max_attempts=int(raw.get('max_attempts', 3)),
			retry_delay=int(raw.get('retry_delay', 2)),
			dlq_enabled=bool(raw.get('dlq_enabled', True)),
		)

	def to_json(self) -> str:
		return json.dumps(self.to_dict(), separators=(',', ':'))

	@classmethod
	def from_json(cls, raw: str) -> 'ForwardJob':
		return cls.from_dict(json.loads(raw))
