import sys
import json
import socket
import threading
import os

def main():
    # Get site ID from arguments
    if len(sys.argv) < 2:
        print("Usage: python kv_server.py <site_id>")
        sys.exit(1)

    site_id = sys.argv[1]

    # Read knownhosts.json
    with open('knownhosts.json', 'r') as f:
        knownhosts = json.load(f)

    hosts = knownhosts['hosts']

    if site_id not in hosts:
        print(f"Site ID {site_id} not found in knownhosts.json")
        sys.exit(1)

    # Get our own host info
    my_host_info = hosts[site_id]
    my_ip = my_host_info['ip_address']
    my_port = my_host_info['udp_start_port']

    # Create UDP socket
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.bind((my_ip, my_port))

    # Initialize data structures
    site_ids = sorted(hosts.keys())
    num_sites = len(site_ids)
    site_index = site_ids.index(site_id)
    site_indices = {site_id: idx for idx, site_id in enumerate(site_ids)}

    # Load persistent state
    key_value_store = load_state(f'kv_store_{site_id}.json', default={})
    log = load_state(f'log_{site_id}.json', default=[])
    matrix_clock = load_state(f'clock_{site_id}.json', default=[[0]*num_sites for _ in range(num_sites)])

    # Lock for thread safety
    lock = threading.Lock()

    # Start listener thread
    listener_thread = threading.Thread(target=listen_udp, args=(udp_sock, key_value_store, log, matrix_clock, site_ids, site_indices, site_id, lock))
    listener_thread.daemon = True
    listener_thread.start()

    # Main loop to accept commands from stdin
    while True:
        try:
            command = input()
            command = command.strip()
            if not command:
                continue

            cmd_parts = command.split()

            if cmd_parts[0] == 'put':
                # Handle put command
                if len(cmd_parts) != 3:
                    print("Invalid put command")
                    continue
                key = cmd_parts[1]
                value = cmd_parts[2]
                with lock:
                    success = handle_put(key_value_store, log, matrix_clock, site_id, site_indices, key, value)
                    # Save state
                    save_state(f'kv_store_{site_id}.json', key_value_store)
                    save_state(f'log_{site_id}.json', log)
                    save_state(f'clock_{site_id}.json', matrix_clock)
                if success:
                    print("put completed")
                else:
                    print("unable to execute put")
            elif cmd_parts[0] == 'get':
                # Handle get command
                if len(cmd_parts) != 2:
                    print("Invalid get command")
                    continue
                key = cmd_parts[1]
                with lock:
                    handle_get(key_value_store, key)
            elif cmd_parts[0] == 'append':
                # Handle append command
                if len(cmd_parts) != 3:
                    print("Invalid append command")
                    continue
                key = cmd_parts[1]
                value = cmd_parts[2]
                with lock:
                    success = handle_append(key_value_store, log, matrix_clock, site_id, site_indices, key, value)
                    # Save state
                    save_state(f'kv_store_{site_id}.json', key_value_store)
                    save_state(f'log_{site_id}.json', log)
                    save_state(f'clock_{site_id}.json', matrix_clock)
                if success:
                    print("append completed")
                else:
                    print("unable to execute append")
            elif cmd_parts[0] == 'remove':
                # Handle remove command
                if len(cmd_parts) != 2:
                    print("Invalid remove command")
                    continue
                key = cmd_parts[1]
                with lock:
                    success = handle_remove(key_value_store, log, matrix_clock, site_id, site_indices, key)
                    # Save state
                    save_state(f'kv_store_{site_id}.json', key_value_store)
                    save_state(f'log_{site_id}.json', log)
                    save_state(f'clock_{site_id}.json', matrix_clock)
                if success:
                    print("remove completed")
                else:
                    print("unable to execute remove")
            elif cmd_parts[0] == 'send':
                # Handle send command
                if len(cmd_parts) != 2:
                    print("Invalid send command")
                    continue
                dest_site_id = cmd_parts[1]
                if dest_site_id not in hosts:
                    print(f"Site ID {dest_site_id} not found")
                    continue
                with lock:
                    handle_send(udp_sock, hosts, dest_site_id, log, matrix_clock, site_id, site_ids, site_indices)
                    # Perform log truncation
                    if truncate_log(log, matrix_clock, site_indices, site_ids):
                        save_state(f'log_{site_id}.json', log)
            elif cmd_parts[0] == 'sendall':
                # Handle sendall command
                with lock:
                    handle_sendall(udp_sock, hosts, log, matrix_clock, site_id, site_ids, site_indices)
                    # Perform log truncation
                    if truncate_log(log, matrix_clock, site_indices, site_ids):
                        save_state(f'log_{site_id}.json', log)
            elif cmd_parts[0] == 'kv':
                # Print key-value store
                with lock:
                    handle_kv(key_value_store)
            elif cmd_parts[0] == 'log':
                # Print log
                with lock:
                    handle_log(log)
            elif cmd_parts[0] == 'clock':
                # Print matrix clock
                with lock:
                    handle_clock(matrix_clock, site_ids)
            else:
                print("Unknown command")
        except EOFError:
            break

def listen_udp(udp_sock, key_value_store, log, matrix_clock, site_ids, site_indices, site_id, lock):
    while True:
        data, addr = udp_sock.recvfrom(65536)
        message = data.decode('utf-8')
        # Process the message
        handle_incoming_message(message, key_value_store, log, matrix_clock, site_ids, site_indices, site_id, lock)
        # Save state after processing message
        with lock:
            save_state(f'kv_store_{site_id}.json', key_value_store)
            save_state(f'log_{site_id}.json', log)
            save_state(f'clock_{site_id}.json', matrix_clock)
            # Perform log truncation
            if truncate_log(log, matrix_clock, site_indices, site_ids):
                save_state(f'log_{site_id}.json', log)

def handle_incoming_message(message, key_value_store, log, matrix_clock, site_ids, site_indices, site_id, lock):
    # Parse the message
    # Message format: JSON with 'matrix_clock' and 'events'
    try:
        msg = json.loads(message)
        sender_site_id = msg['site_id']
        sender_matrix_clock = msg['matrix_clock']
        events = msg['events']
        with lock:
            # Update matrix clock
            update_matrix_clock(matrix_clock, sender_matrix_clock, site_indices)
            # Process events
            for event in events:
                process_event(event, key_value_store, log, matrix_clock, site_ids, site_indices, site_id)
    except Exception as e:
        print(f"Error processing incoming message: {e}")

def process_event(event, key_value_store, log, matrix_clock, site_ids, site_indices, site_id):
    # Event includes 'operation', 'key', 'value', 'timestamp', 'site_id'
    event_site_id = event['site_id']
    event_timestamp = event['timestamp']
    idx_i = site_indices[event_site_id]
    local_idx = site_indices[site_id]

    key = event['key']
    operation = event['operation']
    value = event['value']

    # Check if the key exists and compare timestamps
    if key in key_value_store:
        current_timestamp = key_value_store[key]['timestamp']
        current_site_id = key_value_store[key]['site_id']
        if event_timestamp > current_timestamp or (event_timestamp == current_timestamp and event_site_id > current_site_id):
            apply_event(operation, key, value, event_timestamp, event_site_id, key_value_store)
            log.append(event)
            # Update the matrix clock to reflect that this event has been processed
            matrix_clock[idx_i][local_idx] = max(matrix_clock[idx_i][local_idx], event_timestamp)
    else:
        # Key does not exist
        if operation == 'insert':
            apply_event(operation, key, value, event_timestamp, event_site_id, key_value_store)
            log.append(event)
            # Update the matrix clock to reflect that this event has been processed
            matrix_clock[idx_i][local_idx] = max(matrix_clock[idx_i][local_idx], event_timestamp)

def update_matrix_clock(matrix_clock, sender_matrix_clock, site_indices):
    num_sites = len(matrix_clock)
    for i in range(num_sites):
        for j in range(num_sites):
            matrix_clock[i][j] = max(matrix_clock[i][j], sender_matrix_clock[i][j])

def handle_put(key_value_store, log, matrix_clock, site_id, site_indices, key, value):
    idx = site_indices[site_id]
    # Increment own clock
    matrix_clock[idx][idx] += 1
    event_timestamp = matrix_clock[idx][idx]

    if key in key_value_store:
        # Existing key, cannot perform put
        return False
    else:
        # Create event
        event = {
            'operation': 'insert',
            'key': key,
            'value': [value],
            'timestamp': event_timestamp,
            'site_id': site_id
        }
        # Apply event
        key_value_store[key] = {
            'value': [value],
            'timestamp': event_timestamp,
            'site_id': site_id
        }
        # Add to log
        log.append(event)
        return True

def handle_get(key_value_store, key):
    if key in key_value_store:
        value_list = key_value_store[key]['value']
        value_str = ', '.join(value_list)
        print(f"get {key} returned {value_str}")
    else:
        print(f"get {key} returned null")

def handle_append(key_value_store, log, matrix_clock, site_id, site_indices, key, value):
    idx = site_indices[site_id]
    # Increment own clock
    matrix_clock[idx][idx] += 1
    event_timestamp = matrix_clock[idx][idx]

    if key in key_value_store:
        # Create new value list by appending
        new_value = key_value_store[key]['value'].copy()
        new_value.append(value)
        # Create event
        event = {
            'operation': 'insert',
            'key': key,
            'value': new_value,
            'timestamp': event_timestamp,
            'site_id': site_id
        }
        # Apply event
        key_value_store[key] = {
            'value': new_value,
            'timestamp': event_timestamp,
            'site_id': site_id
        }
        # Add to log
        log.append(event)
    else:
        # Treat append as put if key does not exist
        event = {
            'operation': 'insert',
            'key': key,
            'value': [value],
            'timestamp': event_timestamp,
            'site_id': site_id
        }
        # Update key_value_store
        key_value_store[key] = {
            'value': [value],
            'timestamp': event_timestamp,
            'site_id': site_id
        }
        # Add to log
        log.append(event)
    return True

def handle_remove(key_value_store, log, matrix_clock, site_id, site_indices, key):
    if key not in key_value_store:
        return False
    else:
        idx = site_indices[site_id]
        # Increment own clock
        matrix_clock[idx][idx] += 1
        event_timestamp = matrix_clock[idx][idx]
        
        # Create delete event
        event = {
            'operation': 'delete',
            'key': key,
            'value': [],
            'timestamp': event_timestamp,
            'site_id': site_id
        }
        # Apply event
        del key_value_store[key]
        # Add to log
        log.append(event)
        return True

def apply_event(operation, key, value, timestamp, site_id, key_value_store):
    if operation == 'insert':
        key_value_store[key] = {
            'value': value,
            'timestamp': timestamp,
            'site_id': site_id
        }
    elif operation == 'delete':
        if key in key_value_store:
            del key_value_store[key]

def handle_send(udp_sock, hosts, dest_site_id, log, matrix_clock, site_id, site_ids, site_indices):
    dest_host_info = hosts[dest_site_id]
    dest_ip = dest_host_info['ip_address']
    dest_port = dest_host_info['udp_start_port']
    # Prepare message
    # Send only necessary log entries based on matrix clocks
    events_to_send = select_events_to_send(log, matrix_clock, site_indices, dest_site_id)
    if not events_to_send:
        return  # Nothing to send
    message = {
        'site_id': site_id,
        'matrix_clock': matrix_clock,
        'events': events_to_send
    }
    message_str = json.dumps(message)
    # Send message
    udp_sock.sendto(message_str.encode('utf-8'), (dest_ip, dest_port))

def handle_sendall(udp_sock, hosts, log, matrix_clock, site_id, site_ids, site_indices):
    for dest_site_id in hosts.keys():
        if dest_site_id != site_id:
            handle_send(udp_sock, hosts, dest_site_id, log, matrix_clock, site_id, site_ids, site_indices)

def handle_kv(key_value_store):
    for key in sorted(key_value_store.keys()):
        value_list = key_value_store[key]['value']
        value_str = ', '.join(value_list)
        print(f"{key} : [{value_str}]")

def handle_log(log):
    # Sort log entries based on timestamps and site IDs
    sorted_log = sorted(log, key=lambda e: (e['timestamp'], e['site_id']))
    for event in sorted_log:
        operation = event['operation']
        key = event['key']
        value = event['value']
        value_str = ', '.join(value)
        print(f"{operation} {key} [{value_str}]")

def handle_clock(matrix_clock, site_ids):
    num_sites = len(site_ids)
    for i in range(num_sites):
        row = ' '.join(str(matrix_clock[j][i]) for j in range(num_sites))
        print(row)

def save_state(filename, data):
    with open(filename, 'w') as f:
        json.dump(data, f)

def load_state(filename, default):
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            return json.load(f)
    else:
        return default

def select_events_to_send(log, matrix_clock, site_indices, dest_site_id):
    # Select only events that the destination site has not seen
    dest_idx = site_indices[dest_site_id]
    events_to_send = []
    for event in log:
        event_site_id = event['site_id']
        event_timestamp = event['timestamp']
        event_idx = site_indices[event_site_id]
        if event_timestamp > matrix_clock[event_idx][dest_idx]:
            events_to_send.append(event)
    return events_to_send

def truncate_log(log, matrix_clock, site_indices, site_ids):
    # Identify events that have been acknowledged by all sites
    events_to_keep = []
    for event in log:
        event_site_id = event['site_id']
        event_timestamp = event['timestamp']
        idx_i = site_indices[event_site_id]
        # Check if all sites have seen this event
        acknowledged_by_all = True
        for site in site_ids:
            idx_j = site_indices[site]
            if matrix_clock[idx_i][idx_j] < event_timestamp:
                acknowledged_by_all = False
                break
        if not acknowledged_by_all:
            events_to_keep.append(event)
    # Update the log
    if len(events_to_keep) != len(log):
        log.clear()
        log.extend(events_to_keep)
        return True  # Log was truncated
    return False  # No changes to the log

if __name__ == '__main__':
    main()
