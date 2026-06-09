import logging
import time
from typing import Any, Dict, Optional

from flask import Flask

from alerta.forwarding.service import ForwarderQueueService
from alerta.utils.config import Config
from alerta.utils.logging import Logger

LOG = logging.getLogger('alerta.plugins.forwarder')


class ForwardingWorker:
	def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
		self.config = config or {}

	def run_once(self) -> int:
		backend = ForwarderQueueService._get_backend(self.config)
		jobs = ForwarderQueueService.consume(self.config)
		if not jobs:
			LOG.info('Forward worker found no jobs to process')
			return 0

		processed = 0
		for job in jobs:
			try:
				ForwarderQueueService.execute(job)
				backend.ack(job)
				backend.delete(job)
				processed += 1
			except Exception as e:
				job.attempts += 1
				err = str(e)
				if job.attempts >= job.max_attempts:
					if job.dlq_enabled:
						backend.dead_letter(job, err)
						LOG.error(
							f'Forward worker DLQ [kind={job.kind}] [remote={job.remote}] '
							f'[attempts={job.attempts}] - {err}'
						)
					else:
						backend.ack(job)
						LOG.error(
							f'Forward worker drop [kind={job.kind}] [remote={job.remote}] '
							f'[attempts={job.attempts}] - {err}'
						)
				else:
					# Exponential backoff capped at 60 seconds.
					delay = min(60, max(1, job.retry_delay) ** job.attempts)
					LOG.warning(
						f'Forward worker retry [kind={job.kind}] [remote={job.remote}] '
						f'[attempt={job.attempts}/{job.max_attempts}] [delay={delay}s] - {err}'
					)
					time.sleep(delay)
					backend.retry(job, err)
		return processed

	def run_forever(self, idle_sleep: float = 1.0) -> None:
		while True:
			n = self.run_once()
			if n > 0:
				LOG.info(f'Forward worker processed {n} job(s)')
			if n == 0:
				time.sleep(idle_sleep)

def main() -> None:
	config = Config.get_user_config()
	app = Flask('alerta-forwarding-worker')
	app.config.update(config)
	app.debug = bool(config.get('DEBUG', False))
	Logger().setup_logging(app)

	idle_sleep = float(config.get('FORWARD_WORKER_IDLE_SLEEP', 1.0))
	forwarding_worker = ForwardingWorker(config=config)
	LOG.warning('Forwarding worker started (idle loop active)')
	LOG.info('Starting forwarding worker loop')
	forwarding_worker.run_forever(idle_sleep=idle_sleep)


if __name__ == '__main__':
	main()
