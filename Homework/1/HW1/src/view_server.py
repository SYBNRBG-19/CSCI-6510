import socket
import threading
import json
import sys
import copy
import time
import concurrent.futures

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

class PortManager:
    def __init__(self, port_list):
        self.ports = port_list
        self.available_ports = copy.copy(port_list)
        self.lock = threading.Lock()

    def pop(self):
        with self.lock:
            if not self.available_ports:
                return None
            return self.available_ports.pop(0)

    def push(self, port):
        with self.lock:
            if port in self.ports and port not in self.available_ports:
                self.available_ports.append(port)
                return True
            return False

class ServerStatus:
    def __init__(self, known_hosts, server_id, ip, port_manager):
        self.known_hosts = known_hosts
        self.server_id = server_id
        self.ip = ip
        self.port_manager = port_manager
    
    def ping_server(self, server_id, current_view):
        if server_id not in self.known_hosts:
            return False

        ip, port = get_port_ip(self.known_hosts, server_id)

        print('ping server with', ip, port, file=sys.stderr)

        message = {'type': 'ping', 'server_id': server_id, 'value': current_view}
        message_bytes = json.dumps(message).encode('utf-8')

        # Get an available port from the port manager
        local_port = self.port_manager.pop()
        if local_port is None:
            return False

        # Create a new socket bound to the available port
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            try:
                sock.bind((self.ip, local_port))
                sock.settimeout(0.2)
                sock.sendto(message_bytes, (ip, port))
                response, _ = sock.recvfrom(1024)
                response_data = json.loads(response.decode('utf-8'))

                print('get:', response_data, file=sys.stderr)
                
                if response_data.get('type') == 'pong' and response_data.get('server_id') == server_id:
                    return True
            except socket.timeout:
                return False
            except (json.JSONDecodeError, KeyError):
                return False
            finally:
                # Release the port back to the port manager
                self.port_manager.push(local_port)

        return False

    def check_servers(self, servers, current_view):
        results = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {server_id: executor.submit(self.ping_server, server_id, current_view) for server_id in servers}
            results = [futures[server_id].result() for server_id in servers]

            print('check_servers:', results, file=sys.stderr)
        return results
    
    def ping_servers(self, servers, current_view):
        results = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {server_id: executor.submit(self.ping_server, server_id, current_view) for server_id in servers}
            results = [server_id for server_id in servers if futures[server_id].result()]
            print('ping_servers:', sorted(results), file=sys.stderr)
        return sorted(results)

class ViewServer:
    def __init__(self, known_hosts, ip, port, server_status):
        self.known_hosts = known_hosts
        self.server_status = server_status
        self.servers = set()
        self.current_view = {'primary': None, 'backup': None}
        self.lock = threading.Lock()
        self.start(ip, port)

    def start(self, ip, port):
        self.start_listener(ip, port)
        threading.Thread(target=self.handle_commands, daemon=True).start()

    def start_listener(self, ip, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((ip, port))
        threading.Thread(target=self.listener, daemon=True).start()

    def listener(self):
        while True:
            data, addr = self.sock.recvfrom(4096)
            message = data.decode('utf-8')
            try:
                msg = json.loads(message)
                print('listener:', msg, file=sys.stderr)
                if msg['type'] == 'online':
                    self.handle_online(addr, msg['server_id'])
                elif msg['type'] == 'get_view':
                    self.handle_get_view(addr)
                elif msg['type'] == 'get_view_from_kv':
                    self.handle_get_view_from_kv(addr)
            except Exception:
                continue
    
    def handle_online(self, addr, server):
        if server in known_hosts:
            with self.lock:
                if server not in self.servers:
                    self.servers.add(server)
                    if self.current_view['primary'] == None and self.current_view['backup'] == None:
                        self.current_view['primary'] = server
                        response = {'type': 'online', 'value': 'success', 'primary': self.current_view['primary'], 'backup': self.current_view['backup']}
                    elif self.current_view['primary'] != None and self.current_view['backup'] == None:
                        self.current_view['backup'] = server
                        response = {'type': 'online', 'value': 'success', 'primary': self.current_view['primary'], 'backup': self.current_view['backup']}
                    else:
                        response = {'type': 'online', 'value': 'success', 'primary': self.current_view['primary'], 'backup': self.current_view['backup']}
                else:
                    response = {'type': 'online', 'value': 'repeat', 'primary': self.current_view['primary'], 'backup': self.current_view['backup']}
        else:
            response = {'type': 'online', 'value': 'failed', 'primary': self.current_view['primary'], 'backup': self.current_view['backup']}
        self.sock.sendto(json.dumps(response).encode('utf-8'), addr)

    def handle_get_view(self, addr):
        with self.lock:
            self.update_view()
            response = {'type': 'view', 'primary': self.current_view['primary'],
                        'backup': self.current_view['backup']}
        self.sock.sendto(json.dumps(response).encode('utf-8'), addr)

    def handle_get_view_from_kv(self, addr):
        response = {'type': 'view', 'primary': self.current_view['primary'],
                        'backup': self.current_view['backup']}
        self.sock.sendto(json.dumps(response).encode('utf-8'), addr)
    
    def handle_commands(self):
        while True:
            cmd = sys.stdin.readline().strip()
            if cmd == 'view':
                with self.lock:
                    self.update_view()
                    primary = self.current_view['primary'] if self.current_view['primary'] else 'none'
                    backup = self.current_view['backup'] if self.current_view['backup'] else 'none'
                print(f'primary: {primary} backup: {backup}')
                sys.stdout.flush()
    
    def update_view(self):
        servers = [server for server in self.current_view.values() if server is not None]
        if len(servers) != 0:
            check_result = self.server_status.check_servers(servers, self.current_view)
            if False in check_result:
                ping_result = self.server_status.ping_servers(self.servers, self.current_view)
                fserver = None if len(ping_result) == 0 else ping_result[0]
                sserver = None if len(ping_result) <= 1 else ping_result[1]
                if len(servers) == 1 and self.current_view['backup'] == None:
                    self.current_view['primary'] = fserver
                elif len(servers) == 2 and check_result == [False, True]:
                    self.current_view['primary'] = self.current_view['backup']
                    self.current_view['backup'] = sserver
                elif len(servers) == 2 and check_result == [True, False]:
                    self.current_view['backup'] = sserver
                elif len(servers) == 2 and check_result == [False, False]:
                    self.current_view['primary'] = fserver
                    self.current_view['backup'] = sserver

if __name__ == '__main__':
    server_id = 'view'
    known_hosts = load_known_hosts()

    if server_id not in known_hosts:
        print(f"Unknown Server ID: {server_id}", file=sys.stderr)
        sys.exit(1)

    view_host = known_hosts[server_id]
    view_ip = view_host['ip']
    view_ports = view_host['ports']

    port_manager = PortManager(view_ports)
    server_status = ServerStatus(known_hosts, server_id, view_ip, port_manager)

    view_port = port_manager.pop()

    view_server = ViewServer(known_hosts, view_ip, view_port, server_status)

    while True:
        time.sleep(1)