import subprocess
import psutil
import logging
import time


def simple_executor(cmd):
    try:
        process = subprocess.Popen(cmd, close_fds=True, creationflags=subprocess.CREATE_BREAKAWAY_FROM_JOB)
        return process.pid
    except subprocess.CalledProcessError:
        return None


def command_executor(ip_address, user, command, get_output=True):
    cmd = f'wsl ssh {user}@{ip_address} {command}'

    if get_output:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        stdout = process.communicate()[0]
        return stdout
    else:
        return simple_executor(cmd)


def check_ssh_connection(ip_address, user):
    output = command_executor(ip_address, user, "ss -t 'dport = :ssh or sport = :ssh'")
    output = output.decode('utf-8').split('\n')
    connections = list()
    for idx in range(1, len(output)-1):
        parsed = output[idx].split(":")
        peer_ip = parsed[1].split(" ")[-1]
        peer_port = int(parsed[2])
        connections.append((peer_ip, peer_port))
    return connections


def start_ssh_remote_port_forwarding(ip_address, user, remote_port, local_port):
    cmd = f"-R {remote_port}:localhost:{local_port} -N -v"
    ret = command_executor(ip_address, user, cmd, False)
    time.sleep(5)
    return ret


def kill_all_ssh_connections(ip_address, user):
    command_executor(ip_address, user, 'pkill ssh', False)


def kill_ssh(pid):
    cmd = f'taskkill /F /PID {pid}'
    ret = simple_executor(cmd) is not None
    time.sleep(2)
    return ret


def copy2server(ip_address, user, src, dest):
    cmd = f"wsl scp -r -v /mnt/{src} {user}@{ip_address}:{dest}"
    simple_executor(cmd)


def copy2client(ip_address, user, src, dest):
    cmd = f"wsl scp -r -v {user}@{ip_address}:{src} /mnt/{dest}"
    simple_executor(cmd)


def ssh_exists(pid):
    if pid:
        return psutil.pid_exists(pid)
    return False


def simple_logger(logger_name='root'):
    log = logging.getLogger(logger_name)
    log.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)
    log.addHandler(stream_handler)

    return log


