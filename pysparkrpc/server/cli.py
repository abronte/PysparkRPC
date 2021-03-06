import sys
import os
import time
import argparse

from pysparkrpc.server.logger import logger, configure_logging

PID_PATH = '/var/run/pysparkrpc.pid'

def start(args):
    if status(False):
        print('Pysparkrpc already running.')
    else:
        configure_logging(args.foreground, args.log_level)

        if args.foreground:
            start_server(args)
        else:
            start_background(args)

def start_background(args):
    try:
        pid = os.fork()

        if pid != 0:
            pid_file = open(PID_PATH, 'w')
            pid_file.write(str(pid))
            pid_file.close()
    except OSError as e:
        ## some debug output
        sys.exit(1)
    if pid == 0:
        start_server(args)

def start_server(args):
    import pysparkrpc.server as server
    server.run(host=args.host, port=args.port, auth=args.auth)

def load_pid():
    try:
        return int(open(PID_PATH).read())
    except IOError:
        return False

def status(print_status=True):
    pid = load_pid()

    if pid == False:
        if print_status:
            print('Pysparkrpc server is not running.')

        return False
    else:
        try:
            os.kill(pid, 0)
        except OSError:
            if print_status:
                print('Pysparkrpc server is not running.')

            return False
        else:
            if print_status:
                print('Pysparkrpc server is running. Pid: %d' % pid)

            return True

def stop():
    if status(False):
        pid = load_pid()
        os.kill(pid, 15)

        print('Pysparkrpc server stopped.')
    else:
        print('Pysparkrpc server is not running.')

def main():
    parser = argparse.ArgumentParser(description='Pysparkrpc Server')

    required = parser.add_argument_group('required arguments')
    required.add_argument('action',help='start|stop|status', choices=['start', 'stop', 'status'])

    parser.add_argument('--port', help='Port to bind to. Default: 8765', type=int, default=8765)
    parser.add_argument('--host', help='Host to bind to. Default: 0.0.0.0', type=str, default='0.0.0.0')
    parser.add_argument('--auth', help='Set an authentication token to authenticate the client', type=str, default='')
    parser.add_argument('--foreground', help='Starts server in the foreground', action='store_true')
    parser.add_argument('--log-level', help='Sets logging level. Default: INFO', type=str, default='INFO')

    args = parser.parse_args()

    if args.action == 'start':
        start(args)
    elif args.action == 'stop':
        stop()
    elif args.action == 'status':
        status()

if __name__ == "__main__":
    main()
