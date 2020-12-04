from ssh_connection import SshConnection
from ssh_manager import init_elastic
from ssh_tools import simple_logger


if __name__ == "__main__":
    import argparse
    argp = argparse.ArgumentParser()
    argp.add_argument('--id',
                      help='connection ID',
                      required=True)
    argp.add_argument('-d',
                      help='disable connection',
                      dest='disable',
                      action='store_true',
                      default=False)
    argp.add_argument('-e',
                      help='enable connection',
                      dest='enable',
                      action='store_true',
                      default=False)
    argp.add_argument('-i',
                      help='invalidate connection',
                      dest='invalidate',
                      action='store_true',
                      default=False)
    argp.add_argument('-r',
                      help='resurrect connection',
                      dest='resurrection',
                      action='store_true',
                      default=False)

    args = argp.parse_args()
    log = simple_logger()

    force = args.resurrection
    elastic = init_elastic()
    conn = SshConnection(elastic, es_id=args.id, force=force)
    if force:
        conn.maintain()
    elif args.disable:
        conn.disable()
    elif args.enable:
        conn.enable()
    elif args.invalidate:
        conn.invalidate()
    else:
        conn.maintain()
