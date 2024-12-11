import socket
import threading
import json
import sys
import time

def load_known_hosts():
    with open('knownhosts.json', 'r') as f:
        data = json.load(f)
        known_hosts = {}
        for host_id, host_info in data['hosts'].items():
            ip_address = host_info['ip_address']
            udp_start_port = host_info['udp_start_port']
            udp_end_port = host_info['udp_end_port']
            ports = list(range(udp_start_port, udp_end_port + 1))
            known_hosts[host_id] = {'ip': ip_address, 'ports': ports}
        return known_hosts

def get_port_ip(known_hosts, server_id):
    if server_id not in known_hosts:
        print(f"Unknown Server ID: {server_id}", file=sys.stderr)
        return None, None
    host_info = known_hosts[server_id]
    return host_info['ip'], host_info['ports'][0]

def get_primary():
    message = {'type': 'get_view'}
    sock.sendto(json.dumps(message).encode('utf-8'), get_port_ip(known_hosts, 'view'))
    try:
        sock.settimeout(1)
        data, addr = sock.recvfrom(4096)
        response = data.decode('utf-8')
        msg = json.loads(response)
        if msg['type'] == 'view':
            primary_id = msg['primary']
            return primary_id
    except socket.timeout:
        pass
    return None

def send_request(msg, expect_response=False):
    primary_id = get_primary()
    print('primary_id:', primary_id, file=sys.stderr)
    if not primary_id:
        return False, None
    if primary_id not in known_hosts:
        return False, None
    sock.sendto(json.dumps(msg).encode('utf-8'), get_port_ip(known_hosts, primary_id))
    if expect_response:
        try:
            sock.settimeout(2)
            data, addr = sock.recvfrom(65536)
            response = json.loads(data.decode('utf-8'))
            if response.get('type') == 'success':
                return True, response
            else:
                return False, None
        except socket.timeout:
            return False, None
    else:
        try:
            sock.settimeout(2)
            data, addr = sock.recvfrom(4096)
            response = json.loads(data.decode('utf-8'))
            if response.get('type') == 'success':
                return True
            else:
                return False
        except socket.timeout:
            return False

def main():
    while True:
        cmd_line = sys.stdin.readline().strip()
        if not cmd_line:
            continue
        cmd_parts = cmd_line.split()
        if not cmd_parts:
            continue
        command = cmd_parts[0]
        if command == 'put':
            if len(cmd_parts) != 3:
                print('Invalid put command', file=sys.stderr)
                sys.stdout.flush()
                continue
            key = cmd_parts[1]
            value = cmd_parts[2]
            msg = {'type': 'client_request', 'command': 'put', 'key': key, 'value': value}
            success = send_request(msg)
            if success:
                print('put completed')
            else:
                print('unable to execute put')
            sys.stdout.flush()
        elif command == 'get':
            if len(cmd_parts) != 2:
                print('Invalid get command')
                sys.stdout.flush()
                continue
            key = cmd_parts[1]
            msg = {'type': 'client_request', 'command': 'get', 'key': key}
            success, response = send_request(msg, expect_response=True)
            if success:
                if response['value'] is not None:
                    values = ', '.join(response['value'])
                    print(f'get {key} returned {values}')
                else:
                    print(f'get {key} returned null')
            else:
                print('unable to execute get')
            sys.stdout.flush()
        elif command == 'append':
            if len(cmd_parts) != 3:
                print('Invalid append command', file=sys.stderr)
                sys.stdout.flush()
                continue
            key = cmd_parts[1]
            value = cmd_parts[2]
            msg = {'type': 'client_request', 'command': 'append', 'key': key, 'value': value}
            success = send_request(msg)
            if success:
                print('append completed')
            else:
                print('unable to execute append')
            sys.stdout.flush()
        elif command == 'remove':
            if len(cmd_parts) != 2:
                print('Invalid remove command', file=sys.stderr)
                sys.stdout.flush()
                continue
            key = cmd_parts[1]
            msg = {'type': 'client_request', 'command': 'remove', 'key': key}
            success = send_request(msg)
            if success:
                print('remove completed')
            else:
                print('unable to execute remove')
            sys.stdout.flush()
        else:
            print('unknown command', file=sys.stderr)
            sys.stdout.flush()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: client.py <server_id>", file=sys.stderr)
        sys.exit(1)
    server_id = sys.argv[1]

    known_hosts = load_known_hosts()

    if server_id not in known_hosts:
        print(f"Unknown site ID: {server_id}", file=sys.stderr)
        sys.exit(1)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(get_port_ip(known_hosts, server_id))

    main()
