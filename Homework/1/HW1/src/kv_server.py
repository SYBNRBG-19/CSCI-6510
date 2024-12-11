import socket
import threading
import json
import sys
import time
from threading import Lock

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

class KVServer:
    def __init__(self, server_id, known_hosts, ip, port, reserved_port):
        self.server_id = server_id
        self.known_hosts = known_hosts
        self.ip = ip
        self.reserved_port = reserved_port
        self.role = 'none'
        self.kv_store = {}
        self.lock = Lock()
        self.current_view = {'primary': None, 'backup': None}
        self.start(ip, port)

    def start(self, ip, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((ip, port))
        self.report_to_view_server()
        threading.Thread(target=self.listener, daemon=True).start()
        threading.Thread(target=self.handle_commands, daemon=True).start()
    
    def update_view(self):
        message = {'type': 'get_view_from_kv'}
        message_bytes = json.dumps(message).encode('utf-8')
        # Create a new socket bound to the available port
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            try:
                sock.bind((self.ip, self.reserved_port))
                sock.settimeout(1)
                sock.sendto(message_bytes, get_port_ip(self.known_hosts, 'view'))
                response, _ = sock.recvfrom(1024)
                response_data = json.loads(response.decode('utf-8'))
                print('update_view:', response_data, file=sys.stderr)
                self.update_role(response_data['primary'], response_data['backup'])
            except socket.timeout:
                pass

    def report_to_view_server(self):
        message = {'type': 'online', 'server_id': self.server_id}
        message_bytes = json.dumps(message).encode('utf-8')
        try:
            flag = True
            while flag:
                try:
                    self.sock.settimeout(0.4)
                    self.sock.sendto(message_bytes, get_port_ip(self.known_hosts, 'view'))
                    data, addr = self.sock.recvfrom(4096)
                    message = data.decode('utf-8')
                    msg = json.loads(message)
                    print('get back from view server:', msg, file=sys.stderr)
                    if msg['type'] == 'online':
                        if msg.get('value') == 'success':
                            self.update_role(msg['primary'], msg['backup'])
                            flag = False
                except socket.timeout:
                    pass
                time.sleep(0.5)
        except Exception:
            return

    def update_role(self, new_primary, new_backup):
        with self.lock:
            old_role = self.role
            self.current_view['primary'] = new_primary
            self.current_view['backup'] = new_backup
            print('update_role:', 'primary -', new_primary, 'backup -', new_backup, file=sys.stderr)
            if new_primary == self.server_id:
                self.role = 'primary'
            elif new_backup == self.server_id:
                self.role = 'backup'
            else:
                self.role = 'none'
        if self.role != old_role:
            print(f"I am now {self.role}", file=sys.stderr)
            sys.stdout.flush()
            if new_backup == self.server_id:
                print("Get kv store from primary", file=sys.stderr)
                self.get_kv_store_from_primary(new_primary)

    def get_kv_store_from_primary(self, primary_id):
        if primary_id in self.known_hosts:
            message = {'type': 'get_kv_store'}
            self.sock.sendto(json.dumps(message).encode('utf-8'), (get_port_ip(self.known_hosts, primary_id)))
            try:
                self.sock.settimeout(1)
                data, addr = self.sock.recvfrom(65536)
                response = json.loads(data.decode('utf-8'))
                if response['type'] == 'kv_store':
                    self.kv_store = response['kv_store']
            except socket.timeout:
                print("Failed to get kv_store from primary", file=sys.stderr)
                sys.stdout.flush()
                get_view = {'type': 'get_view'}
                self.sock.sendto(json.dumps(get_view).encode('utf-8'), (get_port_ip(self.known_hosts, 'view')))
        else:
            print("Unknown primary ID", file=sys.stderr)
            sys.stdout.flush()

    def listener(self):
        while True:
            try:
                data, addr = self.sock.recvfrom(65536)
                message = data.decode('utf-8')
                msg = json.loads(message)
                print('listener:', msg, file=sys.stderr)
                if msg['type'] == 'get_kv_store':
                    self.handle_get_kv_store(addr)
                elif msg['type'] == 'client_request':
                    self.handle_client_request(msg, addr)
                elif msg['type'] == 'replicate':
                    self.handle_replication_request(msg, addr)
                elif msg['type'] == 'ping':
                    self.handle_ping(msg, addr)
                elif msg['type'] == 'view':
                    self.handle_backup_change(msg, addr)
            except Exception:
                continue

    def handle_ping(self, msg, addr):
        print('handle_ping', msg['value'], file=sys.stderr)
        self.update_role(msg['value']['primary'], msg['value']['backup'])
        response = {'type': 'pong', 'server_id': self.server_id}
        self.sock.sendto(json.dumps(response).encode('utf-8'), addr)

    def handle_get_kv_store(self, addr):
        if self.role == 'primary':
            response = {'type': 'kv_store', 'kv_store': self.kv_store}
            self.sock.sendto(json.dumps(response).encode('utf-8'), addr)
        else:
            response = {'type': 'fail'}
            self.sock.sendto(json.dumps(response).encode('utf-8'), addr)

    def handle_client_request(self, msg, addr):
        print('self.role =', self.role, file=sys.stderr)
        if self.role != 'primary':
            self.update_view()
            if self.role != 'primary':
                response = {'type': 'fail'}
                self.sock.sendto(json.dumps(response).encode('utf-8'), addr)
                return
        command = msg['command']
        key = msg.get('key')
        value = msg.get('value')
        print('handle_client_request', command, key, value, file=sys.stderr)
        if command == 'put':
            with self.lock:
                self.kv_store[key] = [value]
            self.replicate_to_backup(msg)
            response = {'type': 'success'}
            self.sock.sendto(json.dumps(response).encode('utf-8'), addr)
        elif command == 'get':
            if key in self.kv_store:
                response = {'type': 'success', 'value': self.kv_store[key]}
            else:
                response = {'type': 'success', 'value': None}
            self.sock.sendto(json.dumps(response).encode('utf-8'), addr)
        elif command == 'append':
            with self.lock:
                if key in self.kv_store:
                    self.kv_store[key].append(value)
                else:
                    self.kv_store[key] = [value]
            self.replicate_to_backup(msg)
            response = {'type': 'success'}
            self.sock.sendto(json.dumps(response).encode('utf-8'), addr)
        elif command == 'remove':
            with self.lock:
                if key in self.kv_store:
                    del self.kv_store[key]
            self.replicate_to_backup(msg)
            response = {'type': 'success'}
            self.sock.sendto(json.dumps(response).encode('utf-8'), addr)
        else:
            response = {'type': 'fail'}
            self.sock.sendto(json.dumps(response).encode('utf-8'), addr)

    def replicate_to_backup(self, msg):
        backup_id = self.current_view['backup']
        print('replicate_to_backup:', backup_id, file=sys.stderr)
        if backup_id == self.server_id or not backup_id:
            return
        if backup_id in self.known_hosts:
            replication_msg = {'type': 'replicate', 'command': msg['command'],
                               'key': msg.get('key'), 'value': msg.get('value')}
            self.sock.sendto(json.dumps(replication_msg).encode('utf-8'), (get_port_ip(self.known_hosts, backup_id)))
            try:
                self.sock.settimeout(1)
                data, addr = self.sock.recvfrom(4096)
                response = json.loads(data.decode('utf-8'))
                if response.get('type') != 'ack':
                    pass
            except socket.timeout:
                get_view = {'type': 'get_view'}
                self.sock.sendto(json.dumps(get_view).encode('utf-8'), (get_port_ip(self.known_hosts, 'view')))

    def handle_replication_request(self, msg, addr):
        if self.role != 'backup':
            self.update_view()
            if self.role != 'backup':
                response = {'type': 'fail'}
                self.sock.sendto(json.dumps(response).encode('utf-8'), addr)
                return
        command = msg['command']
        key = msg.get('key')
        value = msg.get('value')
        with self.lock:
            if command == 'put':
                self.kv_store[key] = [value]
            elif command == 'append':
                if key in self.kv_store:
                    self.kv_store[key].append(value)
                else:
                    self.kv_store[key] = [value]
            elif command == 'remove':
                if key in self.kv_store:
                    del self.kv_store[key]
        response = {'type': 'ack'}
        self.sock.sendto(json.dumps(response).encode('utf-8'), addr)
    
    def handle_backup_change(self, msg, addr):
        if msg['type'] == 'view':
            new_primary = msg['primary']
            new_backup = msg['backup']
            self.update_role(new_primary, new_backup)

    def handle_commands(self):
        while True:
            cmd = sys.stdin.readline().strip()
            if cmd == 'kv':
                with self.lock:
                    if not self.kv_store:
                        print('no entries')
                    else:
                        for key in sorted(self.kv_store.keys()):
                            values = ', '.join(self.kv_store[key])
                            print(f'{key} : [{values}]')
                sys.stdout.flush()

def main():
    if len(sys.argv) != 2:
        print("Usage: kv_server.py <server_id>", file=sys.stderr)
        sys.exit(1)
    server_id = sys.argv[1]

    known_hosts = load_known_hosts()

    if server_id not in known_hosts:
        print(f"Unknown Server ID: {server_id}", file=sys.stderr)
        sys.exit(1)

    ip, port = get_port_ip(known_hosts, server_id)

    KVServer(server_id, known_hosts, ip, port, port + 1)
    
    while True:
        time.sleep(1)

if __name__ == '__main__':
    main()
