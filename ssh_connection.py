import logging
import socket
import getpass
import datetime
import psutil
from collections import namedtuple
import subprocess

from ssh_tools import start_ssh_remote_port_forwarding, kill_ssh

SshDetails = namedtuple('SshDetails', 'name, server_ip, server_host, remote_port, local_port')


class SshConnection:

    def __init__(self, elastic, ssh_details=None, es_id=None, force=False):
        self._log = logging.getLogger('root')
        self._id = es_id

        self._server_ip = None if ssh_details is None else ssh_details.server_ip
        self._server_host = None if ssh_details is None else ssh_details.server_host
        self._remote_port = None if ssh_details is None else ssh_details.remote_port
        self._local_port = None if ssh_details is None else ssh_details.local_port
        self._name = None if ssh_details is None else ssh_details.name

        self._es = elastic
        self._force = force
        self._client_node = socket.gethostname()
        self._client_ip = socket.gethostbyname(self._client_node)
        self._client_host = getpass.getuser()
        self._valid = 0
        self._active = 0
        self._enabled = 0

        self._activation_time = None
        self._deactivation_time = None
        self._invalidation_time = None
        self._pid = None

        self.validate_input()
        if self.id:
            self.find_connection()

    @property
    def server_ip(self):
        return self._server_ip

    @property
    def server_host(self):
        return self._server_host

    @property
    def client_node(self):
        return self._client_node

    @property
    def client_ip(self):
        return self._client_ip

    @property
    def client_host(self):
        return self._client_host

    @property
    def valid(self):
        return self._valid

    @property
    def active(self):
        return self._active

    @property
    def activation_time(self):
        return self._activation_time

    @property
    def deactivation_time(self):
        return self._deactivation_time

    @property
    def invalidation_time(self):
        return self._invalidation_time

    @property
    def pid(self):
        return self._pid

    @property
    def id(self):
        return self._id

    @property
    def remote_port(self):
        return self._remote_port

    @property
    def local_port(self):
        return self._local_port

    @property
    def name(self):
        return self._name

    @property
    def enabled(self):
        return self._enabled

    def disable(self):
        if self.id:
            self.find_connection()
            if self.alive():
                self.kill_connection()

            self._enabled = 0
            self.update_es()
        else:
            self._log.error(f'Cannot disable the connection as no ES node ID provided')
            raise AttributeError('Connection ID not provided')

    def enable(self):
        if self.id:
            self.find_connection()
            if not self.enabled and self.valid:
                self.start_connection()
            elif not self.valid:
                self._log.error('Connection invalid')
                raise ConnectionAbortedError('Cannot enable invalidated connection!')
        else:
            self._log.error('Cannot enable the connection as no ES node ID provided')
            raise AttributeError('Connection ID not provided')

    def validate_input(self):
        if self._server_host is None and self._server_ip is None and self._id is None:
            raise AttributeError("Either Server HOST and PORT must be provided or connection ID, none is provided")

    def find_connection(self):
        result = self._es.get_connection(self)
        if result:
            connection_data = result['_source']['ssh']
            self._client_node = connection_data['client_node']
            self._client_ip = connection_data['client_ip']
            self._client_host = connection_data['client_host']
            self._server_ip = connection_data['server_ip']
            self._server_host = connection_data['server_host']
            self._remote_port = connection_data['remote_port']
            self._local_port = connection_data['local_port']
            self._name = connection_data['connection_name']
            self._valid = connection_data['valid']
            self._active = connection_data['active']
            self._enabled = connection_data['enabled']
            self._activation_time = connection_data['activation_time']
            self._deactivation_time = connection_data.get('deactivation_time', None)
            self._invalidation_time = connection_data.get('invalidation_time', None)
            self._pid = connection_data.get('pid', None)
            self._id = connection_data['id']
            self._log.info(f'Connection {self.id} found')
            return connection_data

        else:
            self._log.error(f'Connection not found for ID: {self.id}!')
            raise AttributeError('Connection not found')

    def invalidate(self):
        ret = False
        self.find_connection()
        if self.alive():
            ret = self.kill_connection()
        else:
            self._log.warning(f"SSH connection {self.id} with PID: {self.pid} does not exists")

        if ret:
            self.alive()
            self._invalidation_time = self.deactivation_time
            self._enabled = 0
            self._valid = 0
            self.update_es()

    def alive(self):
        alive = False
        if self._pid:
            alive = psutil.pid_exists(self._pid)

        if alive:
            self._log.info(f'Connection {self.id} active')
            self._activation_time = self._activation_time if self._active else self._now()
            self._active = 1
            self._valid = 1
        else:
            self._log.info(f'Connection {self.id} inactive')
            self._pid = None
            self._active = 0

        return alive

    def maintain(self):
        self.find_connection()
        if self.enabled and self.valid:
            if self.alive():
                self.update_es()
            else:
                self.start_connection()

    def start_connection(self):
        self._log.info(f'Starting connection {self._server_host}@{self._server_ip}: {self._remote_port} -> '
                       f'{self._local_port}')
        self._pid = start_ssh_remote_port_forwarding(self._server_ip, self._server_host,
                                                     self._remote_port, self._local_port)

        self._enabled = int(self.pid > 0)
        self.alive()
        self.update_es()

    def kill_connection(self):
        ret = False
        if self.pid:
            try:
                pid2kill = self.pid
                kill_ssh(self.pid)
                if self.alive():
                    self._log.error(f"SSH connection {self.id} with PID: {pid2kill} not killed!")
                else:
                    self._log.info(f'Killed SSH connection {self.id} with PID: {pid2kill}')
                    ret = True
                    self._deactivation_time = self._now()

            except subprocess.CalledProcessError as ex:
                self._log.exception(ex)
        else:
            self._log.error(f'Cannot kill SSH connection {self.id} as the PID {self.pid} is unknown')
        return ret

    def _now(self):
        return datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

    def update_es(self):
        self._es.update_connection(self)
