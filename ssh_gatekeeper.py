import datetime
import uuid
import logging
import elasticsearch
import elasticsearch.helpers


class SshGateKeeper:

    SSH_DICT = {'client_ip': str,
                'client_node': str,
                'client_host': str,
                'server_ip': str,
                'server_host': str,
                'remote_port': int,
                'local_port': int,
                'connection_name': str,
                'last_updated': str,
                'active': int,
                'valid': int,
                'enabled': int,
                'activation_time': str,
                'deactivation_time': str,
                'invalidation_time': str,
                'id': str,
                'pid': int}

    def __init__(self, elastic_info=None, index=None):
        self._log = logging.getLogger('root')
        self._es_index = index
        self._elastic = elasticsearch.Elasticsearch(hosts=[elastic_info])
        exists = self._elastic.indices.exists(index=self._es_index)
        if not exists:
            raise AttributeError(f'Index {self._es_index} does not exist')

    def update_connection(self, ssh_connection):
        now = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        conn_dict = {'client_ip': ssh_connection.client_ip,
                     'client_node': ssh_connection.client_node,
                     'client_host': ssh_connection.client_host,
                     'server_ip': ssh_connection.server_ip,
                     'server_host': ssh_connection.server_host,
                     'remote_port': ssh_connection.remote_port,
                     'local_port': ssh_connection.local_port,
                     'connection_name': ssh_connection.name,
                     'last_updated': now,
                     'active': ssh_connection.active,
                     'valid': ssh_connection.valid,
                     'enabled': ssh_connection.enabled,
                     'activation_time': ssh_connection.activation_time,
                     'deactivation_time': ssh_connection.deactivation_time,
                     'invalidation_time': ssh_connection.invalidation_time,
                     'id': str(ssh_connection.id or uuid.uuid4()),
                     'pid': ssh_connection.pid}

        data_dict = {'ssh': self._validate_dict(conn_dict)}
        return self._push(data_dict, data_dict['ssh']['id'])

    def get_connection(self, ssh_connection):
        query = {"query": {"bool": {"filter":
                                        [{"match_phrase": {"ssh.id": {"query": ssh_connection.id}}}]
                                    }
                           }
                 }

        query_gen = self._query(query)
        result = list(query_gen)
        if len(result) == 1:
            return result[0]
        elif len(result) > 1:
            self._log.error(f"Found more than 1 SSH connection with ID: {ssh_connection.id}")
            raise IndexError
        else:
            self._log.warning(f"No SSH connection found with ID: {ssh_connection.id}")
            return None

    def get_connection_by_name(self, name):
        query = {"query": {"bool": {"filter":
                                        [{"match_phrase": {"ssh.name": {"query": name}}}]
                                    }
                           }
                 }

        query_gen = self._query(query)
        result = list(query_gen)
        if len(result) == 1:
            return result
        elif len(result) > 1:
            self._log.error(f"Found more than 1 SSH connection with name: {name}")
            raise IndexError
        else:
            self._log.warning(f"No SSH connection found with ID: {name}")
            return None

    def get_valid_connections(self):
        query = {"query": {"bool": {"filter":
                                        [{"match_phrase": {"ssh.valid": {"query": 1}}}]
                                    }
                           }
                 }
        query_gen = self._query(query)
        result = list(query_gen)
        if len(result) > 0:
            return result
        else:
            self._log.warning(f"No valid SSH connections found")
            return None

    def get_all_connections(self):
        query = {"query": {"match_all": {}}}

        query_gen = self._query(query)
        result = list(query_gen)
        if len(result) > 0:
            return result
        else:
            self._log.warning(f"No SSH connections found")
            return []

    def _push(self, data, doc_id):
        res = self._elastic.index(index=self._es_index, id=doc_id, body=data, request_timeout=10)
        return res["_shards"]["successful"] == 1

    def _query(self, query):
        try:
            return elasticsearch.helpers.scan(self._elastic, index=self._es_index, query=query, request_timeout=5)

        except (elasticsearch.ConnectionError, elasticsearch.ConnectionTimeout) as ex:
            self._log.exception(ex)
            self._log.error("Connection to ES server failed!")
            return None

    def _validate_dict(self, in_dict):
        output = dict()
        for key, value in in_dict.items():
            if value is None:
                continue
            if key in self.SSH_DICT.keys():
                if isinstance(value, datetime.datetime):
                    output[key] = value
                else:
                    output[key] = self.SSH_DICT[key](value)
        return output
