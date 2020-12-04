import ssh_tools as st

if __name__ == "__main__":
    import time
    ip = "192.168.0.192"
    host = 'pi'
    src = '/home/marek/Documents/data/test'
    dest = 'c/tmp'

    src_c = 'c/tmp/test'
    dest_s = '/home/marek/Documents/data/test'

    # print(check_ssh_connection(ip, host))
    # pid = start_ssh_remote_port_forwarding(ip, host, 12345, 9200)
    # print(pid)
    # print(check_ssh_connection(ip, host))
    # time.sleep(10)
    # kill_all_ssh_connections(ip, host)
    print(st.kill_ssh(1708))
    # print(check_ssh_connection(ip, host))
    # copy2client(ip, host, src, dest)
    # copy2server(ip, host, src_c, dest_s)