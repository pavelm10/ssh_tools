import ruamel.yaml
from elasticsearch.exceptions import ConnectionError

from ssh_connection import SshConnection, SshDetails
from ssh_gatekeeper import SshGateKeeper
from ssh_support import EsConfig
from ssh_tools import simple_logger


def set_ssh_details(name, config):
    detail = SshDetails(name, config['server_ip'], config['server_host'], config['remote_port'], config['local_port'])
    return detail


def init_elastic():
    try:
        return SshGateKeeper(EsConfig.ES, EsConfig.INDEX)
    except (ConnectionRefusedError, ConnectionError):
        return None


def read_config(cfg_path):
    with open(cfg_path) as stream:
        configs = list(ruamel.yaml.safe_load_all(stream))
        return configs


def get_connections_details(cfg_path):
    connection_list = list()
    configs = read_config(cfg_path)
    for name, config in configs[1].items():
        ssh_detail = set_ssh_details(name, config)
        connection_list.append(ssh_detail)
    return connection_list


def find_all_connections(elastic):
    res = elastic.get_all_connections()
    valid_conns = list()
    for result in res:
        data = result['_source']['ssh']
        valid_conns.append((data['connection_name'], data['id']))
    return valid_conns


def get_new_connections(detail_list, valid_connections):
    valid_names = set([conn[0] for conn in valid_connections])
    cfg_names = set([detail.name for detail in detail_list])
    new_names = cfg_names.difference(valid_names)
    new_details = list()
    for detail in detail_list:
        if detail.name in new_names:
            new_details.append(detail)
    return new_details


def maintain_registered_connections(elastic, valid_connections, log):
    for (_, cid) in valid_connections:
        conn = SshConnection(elastic, es_id=cid)
        try:
            conn.maintain()
        except Exception as ex:
            log.exception(ex)
            continue


def start_new_connections(elastic, new_details, log):
    for detail in new_details:
        conn = SshConnection(elastic, ssh_details=detail)
        try:
            conn.start_connection()
        except Exception as ex:
            log.exception(ex)
            continue


def main(cfg_path, log):
    elastic = init_elastic()
    if elastic is not None:
        valid_connections = find_all_connections(elastic)
        maintain_registered_connections(elastic, valid_connections, log)

        cfg_details = get_connections_details(cfg_path)
        new_details = get_new_connections(cfg_details, valid_connections)
        start_new_connections(elastic, new_details, log)
        return True
    else:
        log.warning('Not connected to elastic')


if __name__ == "__main__":
    import argparse
    import sys

    argp = argparse.ArgumentParser()
    argp.add_argument('--cfg',
                      default='ssh.yml',
                      help='path to the config file')
    args = argp.parse_args()
    log = simple_logger()
    sys.exit(not main(args.cfg, log))
