import sys
import json
import socket
import threading
import queue
import time
import select
from collections import OrderedDict

# Constants
MAX_MESSAGE_SIZE = 65507
MSG_TIMEOUT = 0.05   # Non-blocking receive
COMMAND_TIMEOUT = 2.0
MAX_DELAY = 0.05     # Max message delay assumption (not strictly enforced in this code)

class Site:
    def __init__(self, site_id, m, traitor_status):
        self.site_id = site_id
        self.m = int(m)
        self.is_traitor = (traitor_status == "traitor")

        # Read knownhosts.json
        with open("knownhosts.json","r") as f:
            data = json.load(f)
        self.hosts = data["hosts"]
        self.all_site_ids = sorted(list(self.hosts.keys()))
        if self.site_id not in self.all_site_ids:
            raise ValueError("Site ID not found in knownhosts.json")

        self.n = len(self.all_site_ids)
        self.site_index = self.all_site_ids.index(self.site_id)

        # Extract our listening address
        host_info = self.hosts[self.site_id]
        self.ip_address = host_info["ip_address"]
        self.port = host_info["udp_start_port"]  # single port

        # Precompute addresses of all sites
        self.site_addresses = {}
        for sid in self.all_site_ids:
            info = self.hosts[sid]
            self.site_addresses[sid] = (info["ip_address"], info["udp_start_port"])

        # Setup UDP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.ip_address, self.port))
        self.sock.setblocking(False)

        # Data structures
        self.log = []  # list of (cmd_type, key, [values])
        self.kvstore = {}  # key -> list of values

        # slot_state[slot] = {
        #   "decided": bool,
        #   "decided_value": string command,
        #   "round_values": list of sets of strings, indexed by round
        #   "proposer": which site proposed the command
        #   "executing": bool
        # }
        self.slot_state = {}
        self.next_slot = 0  # next free log slot

        # To keep track of what we proposed locally for a slot (for printing success/failure)
        self.proposed_commands = {}  # slot -> command_str proposed by this site

        # Queues and threading
        self.input_queue = queue.Queue()
        self.running = True

        # Start a thread for reading stdin
        self.stdin_thread = threading.Thread(target=self.read_stdin)
        self.stdin_thread.daemon = True
        self.stdin_thread.start()

        # Start a thread to handle messages
        self.msg_thread = threading.Thread(target=self.recv_loop)
        self.msg_thread.daemon = True
        self.msg_thread.start()

    def read_stdin(self):
        while self.running:
            try:
                line = sys.stdin.readline()
                if not line:
                    continue
                line = line.strip()
                self.input_queue.put(line)
            except:
                pass

    def recv_loop(self):
        while self.running:
            r,_,_ = select.select([self.sock],[],[],MSG_TIMEOUT)
            if r:
                try:
                    data, addr = self.sock.recvfrom(MAX_MESSAGE_SIZE)
                    msg = json.loads(data.decode('utf-8'))
                    self.handle_message(msg, addr)
                except:
                    pass

    def send_message(self, msg, dest_id, slot=None, round_no=None):
        message_type = msg.get("type","")
        if message_type == "dolev_strong":
            # Log sending line
            displayed_command = msg["values"][0] if msg["values"] else ""
            if slot is None:
                slot = msg.get("slot",-1)
            if round_no is None:
                round_no = msg.get("round",-1)
            # Construct send string
            chain = msg.get("chain",[])
            if not chain:
                # commander first send
                send_string = f"{self.site_id}:{displayed_command}"
            else:
                # forwarding send
                send_string = f"{self.site_id}:{chain[0]}"
            print(f"log slot={slot} R={round_no} sent {send_string} to {dest_id}", file=sys.stderr)

        # Actually send message
        out = json.dumps(msg).encode('utf-8')
        self.sock.sendto(out, self.site_addresses[dest_id])

    def handle_message(self, msg, addr):
        msg_type = msg.get("type","")
        if msg_type == "propose":
            self.handle_propose(msg, addr)
        elif msg_type == "dolev_strong":
            self.handle_dolev_strong(msg, addr)

    def handle_propose(self, msg, addr):
        # Propose message sent from a lieutenant to the commander
        command = msg["command"]
        slot = msg.get("slot")
        if slot is None:
            slot = len(self.log)

        # If we are not the commander for this slot, ignore
        if self.commander_for_slot(slot) != self.site_index:
            return

        # Commander starts dolev-strong for this slot if not started
        if slot not in self.slot_state:
            self.slot_state[slot] = {
                "decided": False,
                "decided_value": None,
                "round_values": [set() for _ in range(self.m+1)],
                "proposer": msg.get("proposer"),
                "executing": False
            }

        # Commander: Immediately start dolev-strong at round 0 with this command
        if not self.slot_state[slot]["executing"]:
            self.slot_state[slot]["executing"] = True
            # If traitor commander: send different values to different sites, else send the original command
            if self.is_traitor:
                # traitorous behavior
                for i, sid in enumerate(self.all_site_ids):
                    if i == self.site_index:
                        continue
                    send_val = command if i % 2 == 1 else "append sneak attack"
                    dolev_msg = {
                        "type": "dolev_strong",
                        "slot": slot,
                        "round": 0,
                        "values": [send_val],
                        "origin": self.site_id,
                        "chain": []
                    }
                    self.send_message(dolev_msg, sid, slot, 0)
                # Commander "owns" original command. We'll store original to attempt to decide it.
                # Store the original command in round_values[0]
                self.slot_state[slot]["round_values"][0].add(command)
            else:
                # Honest commander
                for i, sid in enumerate(self.all_site_ids):
                    if i == self.site_index:
                        continue
                    dolev_msg = {
                        "type": "dolev_strong",
                        "slot": slot,
                        "round": 0,
                        "values": [command],
                        "origin": self.site_id,
                        "chain": []
                    }
                    self.send_message(dolev_msg, sid, slot, 0)
                self.slot_state[slot]["round_values"][0].add(command)

            # Start Dolev-Strong finalization thread
            threading.Thread(target=self.run_dolev_strong, args=(slot,)).start()

    def handle_dolev_strong(self, msg, addr):
        slot = msg["slot"]
        round_no = msg["round"]
        values = msg["values"]
        origin = msg["origin"]

        displayed_command = values[0] if values else ""
        print(f"log slot={slot} round={round_no} received {origin}:{displayed_command}", file=sys.stderr)

        if slot not in self.slot_state:
            self.slot_state[slot] = {
                "decided": False,
                "decided_value": None,
                "round_values": [set() for _ in range(self.m+1)],
                "proposer": None,
                "executing": False,
                # Initialize Vi (the values_set)
                "values_set": set()
            }

        # Add received values to round_values[round_no]
        for v in values:
            self.slot_state[slot]["round_values"][round_no].add(v)

        # If round=0 and we are a lieutenant, we start dolev-strong now
        if round_no == 0 and self.commander_for_slot(slot) != self.site_index:
            self.slot_state[slot]["executing"] = True
            # Incorporate these new values into V_i if they are new
            new_values = self.slot_state[slot]["round_values"][0] - self.slot_state[slot]["values_set"]
            self.slot_state[slot]["values_set"].update(new_values)
            # Broadcast only new values if honest
            if not self.is_traitor:
                self.broadcast_new_values(slot, 1, new_values)
            # Start the lieutenant dolev-strong steps
            threading.Thread(target=self.lieutenant_dolev_strong, args=(slot,)).start()

    def broadcast_new_values(self, slot, round_no, values):
        # Broadcast the newly discovered values to all nodes
        # Build the message according to dolev-strong rules
        # We assume we have a chain for logging. For simplicity:
        # Just use the first value for the chain representation.
        if not values:
            return
        values_list = list(values)
        chain_str = [f"{values_list[0]}"]  # for logging purposes

        for sid in self.all_site_ids:
            if sid == self.site_id:
                continue
            dolev_msg = {
                "type": "dolev_strong",
                "slot": slot,
                "round": round_no,
                "values": values_list,
                "origin": self.site_id,
                "chain": chain_str
            }
            self.send_message(dolev_msg, sid, slot, round_no)

    def forward_dolev_strong(self, slot, next_round):
        time.sleep(0.1)
        if self.slot_state[slot]["decided"]:
            return
        if self.is_traitor:
            # Traitor lieutenant does not forward
            return

        # Honest lieutenant:
        old_round_values = self.slot_state[slot]["round_values"][next_round-1]
        if not old_round_values:
            return
        values_to_forward = list(old_round_values)
        for sid in self.all_site_ids:
            if sid == self.site_id:
                continue
            chain_str = [f"{values_to_forward[0]}"]
            dolev_msg = {
                "type": "dolev_strong",
                "slot": slot,
                "round": next_round,
                "values": values_to_forward,
                "origin": self.site_id,
                "chain": chain_str
            }
            self.send_message(dolev_msg, sid, slot, next_round)

    def run_dolev_strong(self, slot):
        # Commander finalization: Wait until m+1 rounds have passed
        time.sleep((self.m+1)*0.5)
        all_values = set()
        for r in range(self.m+1):
            all_values.update(self.slot_state[slot]["round_values"][r])
        decided_value = None
        if all_values:
            decided_value = sorted(all_values)[0]

        self.slot_state[slot]["decided"] = True
        self.slot_state[slot]["decided_value"] = decided_value
        print(f"the log entry is {decided_value}", file=sys.stderr)

        # Apply decided value
        if decided_value is not None:
            self.apply_decided_value(slot, decided_value)

    def lieutenant_dolev_strong(self, slot):
        # According to Dolev-Strong:
        # We run R=1,...,m+1 rounds total (R=1,...,m+1)
        # At each round R:
        #   1. We have received messages from round R-1 in handle_dolev_strong.
        #   2. Discard invalid signatures (not needed here)
        #   3. For each newly received value (not in V_i), add to V_i and send to all.
        # After round m+1, decide.

        # The code that receives messages is in handle_dolev_strong,
        # so at the start of each round we wait a bit for messages to arrive.
        for R in range(1, self.m+2): # rounds 1 through m+1 inclusive
            if self.slot_state[slot]["decided"]:
                break
            # Wait to ensure we have received all round R-1 messages
            time.sleep(0.2)

            # Identify newly discovered values in round R-1
            # Actually, at the end of handle_dolev_strong for round=(R-1),
            # those values are already in round_values[R-1].
            # We now add them to V_i if they are new.
            previously_known = self.slot_state[slot]["values_set"]
            newly_received = self.slot_state[slot]["round_values"][R-1] - previously_known
            
            if newly_received:
                # Update V_i
                self.slot_state[slot]["values_set"].update(newly_received)
                
                # Send only these newly discovered values forward if honest and not decided
                if not self.is_traitor and not self.slot_state[slot]["decided"] and R <= self.m:
                    # We do not need to broadcast after round m+1,
                    # because Dolev-Strong ends at round m+1.
                    self.broadcast_new_values(slot, R, newly_received)

        # After round m+1 is complete, decide:
        time.sleep(0.5)
        Vi = self.slot_state[slot]["values_set"]
        decided_value = None
        if len(Vi) == 1:
            # If exactly one value, choose it
            decided_value = list(Vi)[0]
        else:
            # |Vi| != 1
            decided_value = "append sneak attack"

        self.slot_state[slot]["decided"] = True
        self.slot_state[slot]["decided_value"] = decided_value
        print(f"the log entry is {decided_value}", file=sys.stderr)

        if decided_value is not None:
            self.apply_decided_value(slot, decided_value)

    def apply_decided_value(self, slot, command_str):
        parts = command_str.split()
        cmd = parts[0]
        if cmd == "put":
            if len(parts) < 3:
                # invalid command format
                self.log.append(("insert","??",[]))
            else:
                key = parts[1]
                val = " ".join(parts[2:])
                if key not in self.kvstore:
                    self.kvstore[key] = [val]
                    self.log.append(("insert", key, [val]))
                else:
                    self.log.append(("insert", key, [val]))
        elif cmd == "append":
            if len(parts) < 3:
                # invalid
                self.log.append(("append","??",[]))
            else:
                key = parts[1]
                val = " ".join(parts[2:])
                if key not in self.kvstore:
                    self.kvstore[key] = [val]
                    self.log.append(("insert", key, [val]))
                else:
                    self.kvstore[key].append(val)
                    self.log.append(("append", key, [val]))
        elif cmd == "remove":
            if len(parts) < 2:
                self.log.append(("delete","??",[]))
            else:
                key = parts[1]
                if key in self.kvstore:
                    old_values = self.kvstore[key]
                    del self.kvstore[key]
                    self.log.append(("delete", key, old_values))
                else:
                    self.log.append(("delete", key, []))
        else:
            # Possibly "append sneak attack"
            if cmd == "append" and len(parts)==3 and parts[1]=="sneak" and parts[2]=="attack":
                key = "sneak"
                val = "attack"
                if key not in self.kvstore:
                    self.kvstore[key] = [val]
                    self.log.append(("insert", key, [val]))
                else:
                    self.kvstore[key].append(val)
                    self.log.append(("append", key, [val]))
            else:
                # unknown command
                pass

        # Check if we proposed this command locally:
        if slot in self.proposed_commands:
            proposed = self.proposed_commands[slot]
            original_cmd = proposed.split()[0] if proposed else ""
            # If decided_value == proposed, we print success message
            # else print "unable"
            if command_str == proposed:
                if original_cmd == "put":
                    print("put completed")
                    sys.stdout.flush()
                elif original_cmd == "append":
                    print("append completed")
                    sys.stdout.flush()
                elif original_cmd == "remove":
                    print("remove completed")
                    sys.stdout.flush()
            else:
                if original_cmd == "put":
                    print("unable to execute put")
                    sys.stdout.flush()
                elif original_cmd == "append":
                    print("unable to execute append")
                    sys.stdout.flush()
                elif original_cmd == "remove":
                    print("unable to execute remove")
                    sys.stdout.flush()

        self.next_slot = max(self.next_slot, slot+1)

    def commander_for_slot(self, slot):
        return slot % self.n

    def validate_command_locally(self, command_str):
        # Validate if honest
        if self.is_traitor:
            # traitors do not validate?
            # Just say always valid for traitor?
            return True

        parts = command_str.split()
        if not parts:
            return False
        cmd = parts[0]
        if cmd == "put":
            if len(parts)<3:
                return False
            key = parts[1]
            # put requires key not exist
            if key in self.kvstore:
                return False
        elif cmd == "append":
            if len(parts)<3:
                return False
            # append no strict invalid condition (becomes insert if not exist)
        elif cmd == "remove":
            if len(parts)<2:
                return False
            key = parts[1]
            if key not in self.kvstore:
                return False
        # get, kv, log no replication needed, no validation
        return True

    def wait_for_decision(self, slot, command_str):
        start_time = time.time()
        while time.time() - start_time < 10.0:
            time.sleep(0.5)
            if slot in self.slot_state and self.slot_state[slot]["decided"]:
                # Decided
                # apply_decided_value already printed result if this site proposed
                return
        # timed out
        parts = command_str.split()
        if parts:
            cmd = parts[0]
            if cmd == "put":
                print("unable to execute put")
                sys.stdout.flush()
            elif cmd == "append":
                print("unable to execute append")
                sys.stdout.flush()
            elif cmd == "remove":
                print("unable to execute remove")
                sys.stdout.flush()

    def handle_user_command(self, line):
        parts = line.split()
        if len(parts)==0:
            return
        cmd = parts[0]

        if cmd in ["put","append","remove"]:
            slot = self.next_slot
            if self.commander_for_slot(slot) == self.site_index:
                # This site is commander
                # Validate if honest
                if not self.validate_command_locally(line):
                    print(f"unable to execute {cmd}")
                    sys.stdout.flush()
                    return
                # Commander immediately starts dolev-strong at round=0
                self.slot_state[slot] = {
                    "decided": False,
                    "decided_value": None,
                    "round_values": [set() for _ in range(self.m+1)],
                    "proposer": self.site_id,
                    "executing": True
                }
                self.proposed_commands[slot] = line
                # If traitor: send different values
                if self.is_traitor:
                    for i, sid in enumerate(self.all_site_ids):
                        if i == self.site_index:
                            continue
                        send_val = line if (i % 2 == 1) else "append sneak attack"
                        dolev_msg = {
                            "type":"dolev_strong",
                            "slot": slot,
                            "round":0,
                            "values":[send_val],
                            "origin": self.site_id,
                            "chain":[]
                        }
                        self.send_message(dolev_msg, sid, slot, 0)
                    self.slot_state[slot]["round_values"][0].add(line)
                else:
                    # honest commander
                    for i, sid in enumerate(self.all_site_ids):
                        if i == self.site_index:
                            continue
                        dolev_msg = {
                            "type":"dolev_strong",
                            "slot": slot,
                            "round":0,
                            "values":[line],
                            "origin": self.site_id,
                            "chain":[]
                        }
                        self.send_message(dolev_msg, sid, slot, 0)
                    self.slot_state[slot]["round_values"][0].add(line)

                # start dolev-strong finalization
                threading.Thread(target=self.run_dolev_strong, args=(slot,)).start()

                # wait for decision
                self.wait_for_decision(slot, line)
            else:
                # This site is a lieutenant
                # Validate if honest
                if not self.validate_command_locally(line):
                    print(f"unable to execute {cmd}")
                    sys.stdout.flush()
                    return
                self.proposed_commands[slot] = line
                commander_id = self.all_site_ids[self.commander_for_slot(slot)]
                msg = {
                    "type": "propose",
                    "command": line,
                    "slot": slot,
                    "proposer": self.site_id
                }
                self.send_message(msg, commander_id)
                # wait for decision
                self.wait_for_decision(slot, line)

        elif cmd == "get":
            if len(parts)<2:
                print("get returned null")
                sys.stdout.flush()
                return
            key = parts[1]
            if key in self.kvstore:
                vals = self.kvstore[key]
                if len(vals)==1:
                    print(f"get {key} returned {vals[0]}")
                    sys.stdout.flush()
                else:
                    print(f"get {key} returned {', '.join(vals)}")
                    sys.stdout.flush()
            else:
                print(f"get {key} returned null")
                sys.stdout.flush()

        elif cmd == "kv":
            # Print sorted by key
            for key in sorted(self.kvstore.keys()):
                vals = self.kvstore[key]
                if len(vals)==1:
                    print(f"{key} : [{vals[0]}]")
                    sys.stdout.flush()
                else:
                    print(f"{key} : [{', '.join(vals)}]")
                    sys.stdout.flush()

        elif cmd == "log":
            # print the log
            for entry in self.log:
                ctype, key, vals = entry
                if ctype == "insert":
                    print(f"insert {key} [{', '.join(vals)}]")
                    sys.stdout.flush()
                elif ctype == "append":
                    print(f"append {key} [{', '.join(vals)}]")
                    sys.stdout.flush()
                elif ctype == "delete":
                    print(f"delete {key} [{', '.join(vals)}]")
                    sys.stdout.flush()

        else:
            # Unknown command, ignore
            pass

    def run(self):
        while self.running:
            try:
                line = self.input_queue.get(timeout=0.1)
                self.handle_user_command(line)
            except queue.Empty:
                pass

    def stop(self):
        self.running = False
        self.sock.close()

def main():
    sys.stdout.flush()
    if len(sys.argv) < 4:
        print("Usage: python3 site.py <site_id> <m> <traitor|honest>")
        sys.exit(1)
    site_id = sys.argv[1]
    m = sys.argv[2]
    status = sys.argv[3]
    s = Site(site_id, m, status)
    try:
        s.run()
    except KeyboardInterrupt:
        pass
    finally:
        s.stop()

if __name__ == "__main__":
    main()
