import logging
from typing import TYPE_CHECKING, Any, Optional

from flask import request

from alerta.exceptions import ForwardingLoop
from alerta.forwarding.service import ForwarderQueueService
from alerta.plugins import PluginBase
from alerta.utils.response import base_url

if TYPE_CHECKING:
    from alerta.models.alert import Alert  # noqa


LOG = logging.getLogger('alerta.plugins.forwarder')

X_LOOP_HEADER = 'X-Alerta-Loop'


def append_to_header(origin):
    x_loop = request.headers.get(X_LOOP_HEADER)
    return origin if not x_loop else f'{x_loop},{origin}'


def is_in_xloop(server):
    x_loop = request.headers.get(X_LOOP_HEADER)
    return server in x_loop if server and x_loop else False


class Forwarder(PluginBase):
    """
    Alert and action forwarder for federated Alerta deployments
    See https://docs.alerta.io/en/latest/federated.html
    """

    def pre_receive(self, alert: 'Alert', **kwargs) -> 'Alert':

        if is_in_xloop(base_url()):
            http_origin = request.origin or '(unknown)'  # type: ignore
            raise ForwardingLoop(f'Alert forwarded by {http_origin} already processed by {base_url()}')
        return alert

    def post_receive(self, alert: 'Alert', **kwargs) -> Optional['Alert']:
        async_mode = self.get_config('FORWARDING_MODE', default=False, type=bool, **kwargs)

        for remote, auth, actions in self.get_config('FWD_DESTINATIONS', default=[], type=list, **kwargs):
            if is_in_xloop(remote):
                LOG.debug(f'Forward [action=alerts]: {alert.id} ; Remote {remote} already processed alert. Skip.')
                continue
            if not ('*' in actions or 'alerts' in actions):
                LOG.debug(f'Forward [action=alerts]: {alert.id} ; Remote {remote} not configured for alerts. Skip.')
                continue

            headers = {X_LOOP_HEADER: append_to_header(base_url())}

            LOG.info(f'Forward [action=alerts]: {alert.id} ; {base_url()} -> {remote}')
            try:
                job = ForwarderQueueService.build_alert_job(
                    alert=alert,
                    remote=remote,
                    auth=auth,
                    headers=headers,
                    config=kwargs.get('config', {}),
                )
                if async_mode:
                    LOG.info(f'Forward [action=alerts]: {alert.id} ; Publishing job to queue for remote {remote}')
                    ForwarderQueueService.publish(job, config=kwargs.get('config', {}))
                else:
                    LOG.info(f'Forward [action=alerts]: {alert.id} ; Executing job synchronously for remote {remote}')
                    ForwarderQueueService.execute(job)
            except Exception as e:
                LOG.warning(f'Forward [action=alerts]: {alert.id} ; Failed to forward alert to {remote} - {str(e)}')
                continue
            LOG.debug(f'Forward [action=alerts]: {alert.id} ; queued={async_mode}')

        return alert

    def status_change(self, alert: 'Alert', status: str, text: str, **kwargs) -> Any:
        return

    def take_action(self, alert: 'Alert', action: str, text: str, **kwargs) -> Any:
        async_mode = self.get_config('FORWARDING_MODE', default=False, type=bool, **kwargs)

        if is_in_xloop(base_url()):
            http_origin = request.origin or '(unknown)'  # type: ignore
            raise ForwardingLoop('Action {} forwarded by {} already processed by {}'.format(
                action, http_origin, base_url())
            )

        for remote, auth, actions in self.get_config('FWD_DESTINATIONS', default=[], type=list, **kwargs):
            if is_in_xloop(remote):
                LOG.debug(f'Forward [action={action}]: {alert.id} ; Remote {remote} already processed action. Skip.')
                continue
            if not ('*' in actions or 'actions' in actions or action in actions):
                LOG.debug(f'Forward [action={action}]: {alert.id} ; Remote {remote} not configured for action. Skip.')
                continue

            headers = {X_LOOP_HEADER: append_to_header(base_url())}

            LOG.info(f'Forward [action={action}]: {alert.id} ; {base_url()} -> {remote}')
            try:
                job = ForwarderQueueService.build_action_job(
                    alert_id=alert.id,
                    action=action,
                    text=text,
                    remote=remote,
                    auth=auth,
                    headers=headers,
                    config=kwargs.get('config', {}),
                )
                if async_mode:
                    ForwarderQueueService.publish(job, config=kwargs.get('config', {}))
                else:
                    ForwarderQueueService.execute(job)
            except Exception as e:
                LOG.warning(f'Forward [action={action}]: {alert.id} ; Failed to action alert on {remote} - {str(e)}')
                continue
            LOG.debug(f'Forward [action={action}]: {alert.id} ; queued={async_mode}')

        return alert

    def delete(self, alert: 'Alert', **kwargs) -> bool:
        async_mode = self.get_config('FORWARDING_MODE', default=False, type=bool, **kwargs)

        if is_in_xloop(base_url()):
            http_origin = request.origin or '(unknown)'  # type: ignore
            raise ForwardingLoop(f'Delete forwarded by {http_origin} already processed by {base_url()}')

        for remote, auth, actions in self.get_config('FWD_DESTINATIONS', default=[], type=list, **kwargs):
            if is_in_xloop(remote):
                LOG.debug(f'Forward [action=delete]: {alert.id} ; Remote {remote} already processed delete. Skip.')
                continue
            if not ('*' in actions or 'delete' in actions):
                LOG.debug(f'Forward [action=delete]: {alert.id} ; Remote {remote} not configured for deletes. Skip.')
                continue

            headers = {X_LOOP_HEADER: append_to_header(base_url())}

            LOG.info(f'Forward [action=delete]: {alert.id} ; {base_url()} -> {remote}')
            try:
                job = ForwarderQueueService.build_delete_job(
                    alert_id=alert.id,
                    remote=remote,
                    auth=auth,
                    headers=headers,
                    config=kwargs.get('config', {}),
                )
                if async_mode:
                    ForwarderQueueService.publish(job, config=kwargs.get('config', {}))
                else:
                    ForwarderQueueService.execute(job)
            except Exception as e:
                LOG.warning(f'Forward [action=delete]: {alert.id} ; Failed to delete alert on {remote} - {str(e)}')
                continue
            LOG.debug(f'Forward [action=delete]: {alert.id} ; queued={async_mode}')

        return True  # always continue with local delete even if remote delete(s) fail
