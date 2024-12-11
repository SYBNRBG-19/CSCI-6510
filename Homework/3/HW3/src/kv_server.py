import sys
import json
import threading
import socket
import time
import os

# Constants
TIMEOUT = 0.4  # Updated timeout as per your instruction
MAX_RETRIES = 2  # Total of three tries (initial try + 2 retries)
NO_OP = {'operation': 'NO_OP'}  # Special value for no operation

# Load known hosts
with open('knownhosts.json', 'r') as f:
    known_hosts = json.load(f)['hosts']

site_id = sys.argv[1]
site_info = known_hosts[site_id]

# Load or initialize stable storage
log_filename = f'{site_id}_log.json'
state_filename = f'{site_id}_state.json'
kv_store_filename = f'{site_id}.json'

if os.path.exists(log_filename):
    with open(log_filename, 'r') as f:
        log = json.load(f)
else:
    log = []

if os.path.exists(state_filename):
    with open(state_filename, 'r') as f:
        state = json.load(f)
    # Ensure keys are integers
    state = {int(k): v for k, v in state.items()}
else:
    state = {}

if os.path.exists(kv_store_filename):
    with open(kv_store_filename, 'r') as f:
        kv_store = json.load(f)
else:
    kv_store = {}

# Networking setup
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((site_info['ip_address'], site_info['udp_start_port']))
sock.setblocking(False)

# Threads
stop_event = threading.Event()
message_queue = []

# Mutex for thread safety
mutex = threading.Lock()

# Helper functions
def save_state():
    with open(state_filename, 'w') as f:
        json.dump(state, f)

def save_log():
    with open(log_filename, 'w') as f:
        json.dump(log, f)

def save_kv_store():
    with open(kv_store_filename, 'w') as f:
        json.dump(kv_store, f)

def send_message(message, addr):
    try:
        sock.sendto(json.dumps(message).encode(), addr)
        # Print sent message to stderr
        if message['type'] == 'prepare':
            print(f'sending prepare({message["proposal_number"]}) to all sites', file=sys.stderr)
        elif message['type'] == 'accept':
            print(f'sending accept({message["proposal_number"]}, {message["value"]}) to all sites', file=sys.stderr)
        elif message['type'] == 'decide':
            print(f'sending decided value {message["value"]} for slot {message["slot"]} to all sites', file=sys.stderr)
        elif message['type'] == 'value_request':
            print(f'sending value_request for slot {message["slot"]} to {message["from"]}', file=sys.stderr)
        elif message['type'] == 'value_response':
            print(f'sending value_response for slot {message["slot"]} to {message["from"]}', file=sys.stderr)
        elif message['type'] == 'max_slot_request':
            print(f'sending max_slot_request to {message["from"]}', file=sys.stderr)
        elif message['type'] == 'max_slot_response':
            print(f'sending max_slot_response({message["max_slot"]}) to {message["from"]}', file=sys.stderr)
    except Exception:
        pass  # Ignore exceptions during send

def broadcast_message(message):
    for host_id, info in known_hosts.items():
        addr = (info['ip_address'], info['udp_start_port'])
        send_message(message, addr)

def proposer(command):
    global log, kv_store
    # Fill holes
    fill_holes()
    # Check if command can be executed
    if not can_execute(command):
        print(f'unable to execute {command["operation"]}')
        return
    # Propose the command
    slot = len(log)
    proposal_number = int(time.time() * 1000)
    retries = 0
    while retries <= MAX_RETRIES:
        majority = len(known_hosts) // 2 + 1
        promises = []
        prepare_message = {
            'type': 'prepare',
            'proposal_number': proposal_number,
            'slot': slot,
            'from': site_id
        }
        broadcast_message(prepare_message)
        # Process own prepare message immediately
        acceptor(prepare_message)
        start_time = time.time()
        while time.time() - start_time < TIMEOUT:
            with mutex:
                for msg in message_queue:
                    if msg['type'] == 'promise' and msg['proposal_number'] == proposal_number and msg['slot'] == slot:
                        promises.append(msg)
                        message_queue.remove(msg)
            if len(promises) >= majority:
                break
            time.sleep(0.01)
        if len(promises) < majority:
            retries += 1
            proposal_number += 1
            continue
        # Determine highest-numbered proposal
        highest_accepted = None
        for p in promises:
            if p.get('accepted_id') is not None:
                if highest_accepted is None or p['accepted_id'] > highest_accepted['accepted_id']:
                    highest_accepted = p
        if highest_accepted and highest_accepted.get('accepted_value'):
            # Adopt the highest accepted value
            command = highest_accepted['accepted_value']
        accept_message = {
            'type': 'accept',
            'proposal_number': proposal_number,
            'slot': slot,
            'value': command,
            'from': site_id
        }
        broadcast_message(accept_message)
        # Process own accept message immediately
        acceptor_accept(accept_message)
        accepts = []
        start_time = time.time()
        while time.time() - start_time < TIMEOUT:
            with mutex:
                for msg in message_queue:
                    if msg['type'] == 'accepted' and msg['proposal_number'] == proposal_number and msg['slot'] == slot:
                        accepts.append(msg)
                        message_queue.remove(msg)
            if len(accepts) >= majority:
                break
            time.sleep(0.01)
        if len(accepts) >= majority:
            # Commit the value
            decision_message = {
                'type': 'decide',
                'slot': slot,
                'value': command,
                'from': site_id
            }
            broadcast_message(decision_message)
            # Process own decide message immediately
            learner(decision_message)
            print(f'{command["operation"]} completed')
            return
        else:
            retries += 1
            proposal_number += 1
    print(f'unable to execute {command["operation"]}')

def acceptor(message):
    slot = message['slot']
    proposal_number = message['proposal_number']
    with mutex:
        if slot not in state:
            state[slot] = {'promised_id': None, 'accepted_id': None, 'accepted_value': None}
        if state[slot]['promised_id'] is None or proposal_number >= state[slot]['promised_id']:
            state[slot]['promised_id'] = proposal_number
            save_state()
            response = {
                'type': 'promise',
                'proposal_number': proposal_number,
                'slot': slot,
                'accepted_id': state[slot]['accepted_id'],
                'accepted_value': state[slot]['accepted_value'],
                'from': site_id
            }
            sender_info = known_hosts[message['from']]
            addr = (sender_info['ip_address'], sender_info['udp_start_port'])
            send_message(response, addr)
    # Print received prepare message to stderr
    print(f'received prepare({proposal_number}) from {message["from"]}', file=sys.stderr)

def acceptor_accept(message):
    slot = message['slot']
    proposal_number = message['proposal_number']
    with mutex:
        if slot not in state:
            state[slot] = {'promised_id': None, 'accepted_id': None, 'accepted_value': None}
        if state[slot]['promised_id'] is None or proposal_number >= state[slot]['promised_id']:
            state[slot]['promised_id'] = proposal_number
            state[slot]['accepted_id'] = proposal_number
            state[slot]['accepted_value'] = message['value']
            save_state()
            response = {
                'type': 'accepted',
                'proposal_number': proposal_number,
                'slot': slot,
                'from': site_id
            }
            sender_info = known_hosts[message['from']]
            addr = (sender_info['ip_address'], sender_info['udp_start_port'])
            send_message(response, addr)
    # Print received accept request to stderr
    print(f'received accept({proposal_number}, {message["value"]}) from {message["from"]}', file=sys.stderr)

def learner(message):
    slot = message['slot']
    value = message['value']
    with mutex:
        if slot >= len(log):
            log.extend([None] * (slot - len(log) + 1))
        if log[slot] is None:
            log[slot] = value
            save_log()
            # Update KV store
            if value.get('operation') != 'NO_OP':
                apply_to_kv_store(value)
                save_kv_store()
    # Print received decide message to stderr
    print(f'received decided value {value} for slot {slot}', file=sys.stderr)

def receiver():
    while not stop_event.is_set():
        try:
            data, addr = sock.recvfrom(4096)
            message = json.loads(data.decode())
            if message['type'] == 'prepare':
                acceptor(message)
            elif message['type'] == 'accept':
                acceptor_accept(message)
            elif message['type'] == 'decide':
                learner(message)
            elif message['type'] == 'value_request':
                send_value_response(message['slot'], message['from'])
                # Print received message to stderr
                print(f'received value_request for slot {message["slot"]} from {message["from"]}', file=sys.stderr)
            elif message['type'] == 'value_response':
                with mutex:
                    message_queue.append(message)
                # Print received message to stderr
                print(f'received value_response for slot {message["slot"]} from {message["from"]}', file=sys.stderr)
            elif message['type'] == 'max_slot_request':
                send_max_slot_response(message['from'])
                # Print received message to stderr
                print(f'received max_slot_request from {message["from"]}', file=sys.stderr)
            elif message['type'] == 'max_slot_response':
                with mutex:
                    message_queue.append(message)
                # Print received message to stderr
                print(f'received max_slot_response({message["max_slot"]}) from {message["from"]}', file=sys.stderr)
            elif message['type'] == 'promise' or message['type'] == 'accepted':
                with mutex:
                    message_queue.append(message)
            else:
                pass  # Ignore unknown message types
        except socket.error:
            time.sleep(0.01)

def send_value_response(slot, requester):
    with mutex:
        if slot < len(log) and log[slot] is not None:
            value = log[slot]
            response = {
                'type': 'value_response',
                'slot': slot,
                'value': value,
                'from': site_id
            }
            sender_info = known_hosts[requester]
            addr = (sender_info['ip_address'], sender_info['udp_start_port'])
            send_message(response, addr)

def send_max_slot_response(requester):
    with mutex:
        max_slot = len(log) - 1
        response = {
            'type': 'max_slot_response',
            'max_slot': max_slot,
            'from': site_id
        }
        sender_info = known_hosts[requester]
        addr = (sender_info['ip_address'], sender_info['udp_start_port'])
        send_message(response, addr)

def fill_holes():
    max_slot = get_max_slot()
    for i in range(len(log), max_slot + 1):
        log.append(None)
    for i in range(len(log)):
        if log[i] is None:
            # Request the decided value for the slot
            value = request_value(i)
            if value is not None:
                learner({'slot': i, 'value': value})
            else:
                # No decided value; propose NO_OP to fill the slot
                propose_no_op(i)

def get_max_slot():
    max_slot = len(log) - 1
    for host_id, info in known_hosts.items():
        if host_id == site_id:
            continue
        addr = (info['ip_address'], info['udp_start_port'])
        request_message = {
            'type': 'max_slot_request',
            'from': site_id
        }
        send_message(request_message, addr)
    # Process own max_slot_response immediately
    message_queue.append({'type': 'max_slot_response', 'max_slot': len(log) - 1, 'from': site_id})
    start_time = time.time()
    while time.time() - start_time < TIMEOUT:
        with mutex:
            for msg in message_queue:
                if msg['type'] == 'max_slot_response':
                    if msg['max_slot'] > max_slot:
                        max_slot = msg['max_slot']
                    message_queue.remove(msg)
        time.sleep(0.01)
    return max_slot

def request_value(slot):
    for host_id, info in known_hosts.items():
        if host_id == site_id:
            continue
        addr = (info['ip_address'], info['udp_start_port'])
        request_message = {
            'type': 'value_request',
            'slot': slot,
            'from': site_id
        }
        send_message(request_message, addr)
    # Process own value immediately if available
    with mutex:
        if slot < len(log) and log[slot] is not None:
            message_queue.append({'type': 'value_response', 'slot': slot, 'value': log[slot], 'from': site_id})
    start_time = time.time()
    responses = []
    while time.time() - start_time < TIMEOUT:
        with mutex:
            for msg in message_queue:
                if msg['type'] == 'value_response' and msg['slot'] == slot:
                    responses.append(msg)
                    message_queue.remove(msg)
        if responses:
            break
        time.sleep(0.01)
    if responses:
        return responses[0]['value']
    else:
        return None

def propose_no_op(slot):
    proposal_number = int(time.time() * 1000)
    retries = 0
    command = {'operation': 'NO_OP'}
    while retries <= MAX_RETRIES:
        majority = len(known_hosts) // 2 + 1
        promises = []
        prepare_message = {
            'type': 'prepare',
            'proposal_number': proposal_number,
            'slot': slot,
            'from': site_id
        }
        broadcast_message(prepare_message)
        acceptor(prepare_message)
        start_time = time.time()
        while time.time() - start_time < TIMEOUT:
            with mutex:
                for msg in message_queue:
                    if msg['type'] == 'promise' and msg['proposal_number'] == proposal_number and msg['slot'] == slot:
                        promises.append(msg)
                        message_queue.remove(msg)
            if len(promises) >= majority:
                break
            time.sleep(0.01)
        if len(promises) < majority:
            retries += 1
            proposal_number += 1
            continue
        accept_message = {
            'type': 'accept',
            'proposal_number': proposal_number,
            'slot': slot,
            'value': command,
            'from': site_id
        }
        broadcast_message(accept_message)
        acceptor_accept(accept_message)
        accepts = []
        start_time = time.time()
        while time.time() - start_time < TIMEOUT:
            with mutex:
                for msg in message_queue:
                    if msg['type'] == 'accepted' and msg['proposal_number'] == proposal_number and msg['slot'] == slot:
                        accepts.append(msg)
                        message_queue.remove(msg)
            if len(accepts) >= majority:
                break
            time.sleep(0.01)
        if len(accepts) >= majority:
            decision_message = {
                'type': 'decide',
                'slot': slot,
                'value': command,
                'from': site_id
            }
            broadcast_message(decision_message)
            learner(decision_message)
            break
        retries += 1
        proposal_number += 1

def can_execute(command):
    op = command['operation']
    key = command['key']
    if op == 'put':
        return key not in kv_store
    elif op == 'append':
        return True
    elif op == 'remove':
        return key in kv_store
    else:
        return False

def apply_to_kv_store(command):
    op = command['operation']
    key = command['key']
    value = command.get('value')
    if op == 'put':
        kv_store[key] = [value]
    elif op == 'append':
        if key in kv_store:
            kv_store[key].append(value)
        else:
            kv_store[key] = [value]
    elif op == 'remove':
        if key in kv_store:
            del kv_store[key]

def main():
    threading.Thread(target=receiver, daemon=True).start()
    # Wait for 1 second before processing commands
    time.sleep(1)
    # On startup, fill any holes in the log and rebuild the kv_store
    fill_holes()
    rebuild_kv_store()
    while True:
        try:
            user_input = input()
            if not user_input.strip():
                continue
            time.sleep(0.3)  # Delay between commands
            tokens = user_input.strip().split()
            if tokens[0] == 'put' and len(tokens) >= 3:
                key = tokens[1]
                value = ' '.join(tokens[2:])
                command = {
                    'operation': 'put',
                    'key': key,
                    'value': value
                }
                proposer(command)
            elif tokens[0] == 'get' and len(tokens) == 2:
                key = tokens[1]
                if key in kv_store:
                    values = kv_store[key]
                    value_str = ', '.join(values)
                    print(f'get {key} returned {value_str}')
                else:
                    print(f'get {key} returned null')
            elif tokens[0] == 'append' and len(tokens) >= 3:
                key = tokens[1]
                value = ' '.join(tokens[2:])
                command = {
                    'operation': 'append',
                    'key': key,
                    'value': value
                }
                proposer(command)
            elif tokens[0] == 'remove' and len(tokens) == 2:
                key = tokens[1]
                if key in kv_store:
                    value = kv_store[key]  # Value being removed
                    command = {
                        'operation': 'remove',
                        'key': key,
                        'value': value  # Include value being removed
                    }
                    proposer(command)
                else:
                    print('unable to execute remove')
            elif tokens[0] == 'kv':
                for key in sorted(kv_store.keys()):
                    values = kv_store[key]
                    value_str = ', '.join(values)
                    print(f'{key} : [{value_str}]')
            elif tokens[0] == 'log':
                for entry in log:
                    if entry and entry.get('operation') != 'NO_OP':
                        op = entry['operation']
                        key = entry['key']
                        value = entry.get('value')
                        if op == 'put':
                            print(f'insert {key} [{value}]')
                        elif op == 'append':
                            print(f'append {key} [{value}]')
                        elif op == 'remove':
                            removed_value = ', '.join(value)
                            print(f'delete {key} [{removed_value}]')
            else:
                print('Invalid command')
        except EOFError:
            break
        except Exception:
            pass  # Ignore other exceptions

def rebuild_kv_store():
    global kv_store
    kv_store = {}
    with mutex:
        for entry in log:
            if entry and entry.get('operation') != 'NO_OP':
                apply_to_kv_store(entry)
    save_kv_store()

if __name__ == '__main__':
    main()
    stop_event.set()
    sock.close()
