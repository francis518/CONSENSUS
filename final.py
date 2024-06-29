#written by Ochiagha ifeanyi
#course: 3010
#lecturer : Zach havens



import socket
import json
import sys
import select
import json
import time
import random
import uuid
import re
import threading

all_message_ids = []
global word_freq 
word_freq = {}

last_gossip_time = time.time()
peer_gossip_times = {}
last_words = {}
lie_state = False

word_list = {
    0: "word1",
    1: "word2",
    2: "word3",
    3: "word4",
    4: "word5"
}

PEERS = [
    ('130.179.28.127', 16000),
    ('130.179.28.113', 16000),
    ('130.179.28.119', 16000),
    ('130.179.28.126', 16000)
]

#this is adding the initial well know peers and setting their last word to none
for peer_ip, peer_port in PEERS:
    last_words[(peer_ip, peer_port)] = None


#this function starts off the program by sending a gossip message to all well known pairs
def announce(s):
    global PEERS
    gossip_message = {
        "command": "GOSSIP",
        "host": socket.getfqdn(),
        "port": s.getsockname()[1],
        "name": "franco",
        "messageID": str(uuid.uuid4())
        }
    
    for peer in PEERS:
        try:
            s.sendto(json.dumps(gossip_message).encode(), peer)
            print(f"Gossip message sent to {peer}")
        except Exception as e:
            print(f"Error sending gossip message to {peer}: {e}")




def listen(s):
    global PEERS
    global last_gossip_time
    
    try:
        while True:
            current_time = time.time()
            if current_time - last_gossip_time >= 60:
                announce(s)
                check_inactive_peers() #this checks for any peers that have not sent any gossip messages for 2 minutes or more
                last_gossip_time = current_time# this is the last_gossip_time for my own node
          
            data, addr = s.recvfrom(1024)
            message = data.decode('utf-8')
          
            try:
                message_dict = json.loads(message)
                if isinstance(message_dict, dict) and "command" in message_dict:
                    #this checks if the command protocol is goossip reply
                    if message_dict["command"] == "GOSSIP_REPLY":
                        print(f"Received gossip reply message from {addr}")
                        peer_gossip_times[addr] = time.time()
                        if(addr not in PEERS):
                            PEERS.append(addr)
                           
                    
                    
                    elif message_dict["command"] == "GOSSIP":
                        if(addr not in PEERS):
                            PEERS.append(addr)
                            
                        
                        command = message_dict["command"]
                        host = message_dict["host"]
                        port = message_dict["port"]
                        name = message_dict["name"]
                        messageID = message_dict["messageID"]
                    
                        if(messageID not in all_message_ids):
                                random_gossip(s,message)
                                
                        all_message_ids.append(messageID)
                    
                        gossip_reply = {
                            "command": "GOSSIP_REPLY",
                            "host": host,
                            "port": s.getsockname()[1],
                            "name": "franco"
                        }
                    
                        peer_gossip_times[addr] = time.time()

                    
                        try:
                            s.sendto(json.dumps(gossip_reply).encode(), addr)
                            print(f"Gossip message reply sent to {addr}")
                        
                   
                        except Exception as e:
                            print(f"Error sending gossip message to {addr}: {e}")
                
                
                
                    elif message_dict["command"] == "CONSENSUS":
                        
                        try:
                            if all(key in message_dict for key in ("command", "OM", "index", "value", "peers", "messageID", "due")):

                                command = message_dict["command"]
                                om_level = message_dict["OM"]
                                index = message_dict["index"]
                                word = message_dict["value"]
                                peers = message_dict["peers"]
                                messageID = message_dict["messageID"]
                                due_time = message_dict["due"]

                                global word_freq
                    
                    
                                #this checks if the om level is 0
                                if(int(om_level) == 0 and isinstance(peers, list)):
                                    consensus_reply(s,addr,index,messageID)
                    
                                elif(int(om_level) > 0 and isinstance(peers, list)):
                                    sub_consensus(s,index,word,om_level,peers,due_time,addr,messageID)
                        
                                else:
                                    s.sendto(json.dumps(" ").encode(), addr)
                            
                        
                                #this adds the message id to the list of messages ids to avoid replying more than once to the same gossip
                                    all_message_ids.append(messageID) 
                                    
                        
                        # Proceed with further processing
                            else:
                                print("Missing one or more required fields in the message.")
                        
                        except KeyError as e:
                            print(f"Missing required field in the message: {e}")

                
                    elif message_dict["command"] == "QUERY":
                        for word in word_list:
                            s.sendto(json.dumps(word).encode(), addr)
                   
                
                
            except json.JSONDecodeError:
                print("Received data is not valid JSON:", message)
    
    except KeyboardInterrupt:
        s.close()  
        
    except OSError:
        pass
          
 
def add_last_word(addr,word):
    last_words[addr] = word
 

#this forwards the gossip message to random peers if messageId not in messageId list            
def random_gossip(s,message):
    random_peers = random.sample(PEERS, min(5, len(PEERS)))
    for peers in random_peers:
        s.sendto(json.dumps(message).encode(), peers)
    

#this prints all the peers in the active peer list
def print_all_peers():
    print("All Known Peers:")
    for address, message in PEERS.items():
        print(f"Peer Address: {address}, Message: {message}")
  
 
  
def check_inactive_peers():
    current_time = time.time()
    inactive_threshold = 120  

    for peer, last_time in peer_gossip_times:
        if current_time - last_time >= inactive_threshold and peer in PEERS:
            PEERS.remove(peer)
            
        
def set(index, word):
    if word is not None:
        word_list[index] = word


def get_om(num_peers):
    return_value = int((num_peers - 1)/3)
    return return_value


#this starts a consensus with you as the commander
def start_consensus(index, s):
    global word_freq
    word_freq = {}
    
    new_id = str(uuid.uuid4())
    
    theTime = time.time() + 70
    copy = PEERS.copy()
    if len(copy) != 0 and socket.getfqdn() in copy:
        copy.remove(socket.getfqdn())
     
    num_expected_replies = len(copy)
    om = get_om(num_expected_replies)
   
    send = {
        "command": "CONSENSUS",
        "OM": om,
        "index": index,
        "value": word_list[index],
        "peers": copy,
        "messageID": new_id,
        "due": theTime
    }
   
   
    for peer in copy:
        host, port = peer
        s.sendto(json.dumps(send).encode(), (host, port))
 
    curr_replies = 0
    
    while time.time() < theTime:
        time_remaining = theTime - time.time()
        if time_remaining <= 0:
            break
        
        data_ready, _, _ = select.select([s], [], [],time_remaining)
        
        if data_ready:
            data, addr = s.recvfrom(1024)
            message = data.decode('utf-8')
            
            try:
                message_dict = json.loads(message)
                if message_dict["command"] == "CONSENSUS-REPLY":
                    word = message_dict["value"]
                    word_freq[word] = word_freq.get(word, 0) + 1
                    add_last_word(addr,word)
                    print(f"Received consensus result from {addr}: {word}")
                    print("Current word frequencies:", word_freq)
                    curr_replies = curr_replies + 1
                    
                    
                    if curr_replies == num_expected_replies:
                        break
            
            except json.JSONDecodeError:
                print("Received data is not valid JSON:", message)


    #this checks if a consensus reply was received from all the active peers
    if curr_replies == num_expected_replies:
        highest_frequency = max(word_freq, key=word_freq.get)
        print(f"Word with highest frequency is {highest_frequency}")
        set(index, highest_frequency)
    
    #if not everyone replied it just shows this message and consensus does not continue
    else:
        print("Not all peers replied to consensus")
        
    print("Consensus is over")
    

    

def consensus_reply(s,address,index, ID):
    reply = {
        "command": "CONSENSUS-REPLY",
        "value": word_list[index],
        "reply-to": ID
        
    }
    
    lie = {
        "command": "CONSENSUS-REPLY",
        "value": "LIE",
        "reply-to": ID
    }
    
    #this uses the current lie_state to determine if lie is told to commander 
    if(lie_state == False):
        s.sendto(json.dumps(reply).encode(), address)
    else:
        s.sendto(json.dumps(lie).encode(), address)
        
    print("word sent")


#this handles running a sub consensus when the om level is greater than 0
def sub_consensus(s,index,value,om,peers,timedue, cmaddress,messageid):
    global word_freq
    word_freq = {}
    
    if len(peers) != 0 and socket.getfqdn() in peers:
        peers.remove(socket.getfqdn())
    
    curr_om_level = om - 1
    new_id = str(uuid.uuid4())
    due = timedue - 50
    num_expected_replies = len(peers)
    
    
    send_consensus = { "command": "CONSENSUS",
        "OM": curr_om_level,
        "index": index,
        "value": value,
        "peers": peers,
        "messageID": new_id,
        "due":due
    }
    
    for peer in peers:
        host, port_str = peer.split(":")
        port = int(port_str)
        s.sendto(json.dumps(send_consensus).encode(), (host, port))

 
    curr_replies = 0
    
    while time.time() < due:
        time_remaining = due - time.time()
        if time_remaining <= 0:
            break
        
        data_ready, _, _ = select.select([s], [], [],time_remaining)
        
        if data_ready:
            data, addr = s.recvfrom(1024)
            message = data.decode('utf-8')
            
            try:
                message_dict = json.loads(message)
                if message_dict["command"] == "CONSENSUS-REPLY":
                    word = message_dict["value"]
                    word_freq[word] = word_freq.get(word, 0) + 1
                    add_last_word(addr,word)
                    print(f"Received consensus result from {addr}: {word}")
                    print("Current word frequencies:", word_freq)
                    curr_replies = curr_replies + 1
                    
                    
                    if curr_replies == num_expected_replies:
                        break
            
            except json.JSONDecodeError:
                print("Received data is not valid JSON:", message)


    if curr_replies == num_expected_replies:
        highest_frequency = max(word_freq, key=word_freq.get)
        print(f"Word with highest frequency is {highest_frequency}")
        set(index, highest_frequency)
        reply = {
            "command": "CONSENSUS-REPLY",
            "value": highest_frequency,
            "reply-to": messageid
        }
        cap = {
            "command": "CONSENSUS-REPLY",
            "value": "CAP",
            "reply-to": messageid
        }
        
        if(lie_state == False):
            s.sendto(json.dumps(reply).encode(), cmaddress)
        else:
            s.sendto(json.dumps(cap).encode(), cmaddress)
        
    else:
        print("Not all peers replied to consensus")
        temp_word = "response not received"
        s.sendto(json.dumps(temp_word).encode(), cmaddress)
        
    print("Consensus is over")
    
    
    
    
    
 #this handles the command line interface for users    
def start_cli_server(server_socket, udp):
    
    sockets = [server_socket]
    
    available_commands = """Available commands:
    peers - list all known peers, with last words received from each.
    current - the current word list.
    consensus x - where x is a numeric index. Run a consensus for this word index.
    lie - begin lying. Lie either all the time, or a set percentage of the time.
    truth - stop lying. Always return 'honest' answers.
    set x y - where x is an index, and y is a word. Set the value x to word y.
    exit - close the command-line interface."""
    
    try:
        while True:
            
            readable, _, _ = select.select(sockets, [], [])
            
            for sock in readable:
                if sock is server_socket:  # Handle incoming connections
                    client_socket, client_address = server_socket.accept()
                    print(f"Connection from {client_address}")
                    client_socket.send(available_commands.encode())
                    sockets.append(client_socket)  # Add client socket to list of monitored sockets
                    
                else:  # Handle data from client
                    data = sock.recv(1024).decode().strip()
                    if not data:  # If no data received, client has closed the connection
                        print(f"Client {sock.getpeername()} disconnected")
                        sockets.remove(sock)  # Remove client socket from list of monitored sockets
                        sock.close()
                    else:
                        try:
                            command_parts = data.split()
                            command = command_parts[0]
                            arguments = command_parts[1:]

                            if command == 'peers':
                                last_words_str_keys = {str(key): value for key, value in last_words.items()}
                            # JSON encode the dictionary
                                lastword_list = json.dumps(last_words_str_keys)
                                sock.sendall(lastword_list.encode())
                           
                            elif command == 'current':
                            
                                word_list_json = json.dumps(word_list)
                                sock.sendall(word_list_json.encode())
                                pass
                            elif command == 'consensus':
                                if arguments:  # Check if arguments list is not empty
                                    index = int(arguments[0])  # Convert the argument to an integer
                                    print(index)
                                    start_consensus(udp, index)
                                else:
                                    pass
                            
                            
                            elif command == 'lie':
                                lie_state = True
                            
                            elif command == 'truth':
                                lie_state = False
                            
                            elif command == 'set':
                                index_val = int(arguments[0])
                                word = arguments[1]
                                set(index_val,word)
                            
                            elif command == 'exit':
                                print("Exiting the command-line interface.")
                                sockets.remove(sock) 
                                sock.close()
                                break
                        
                            else:
                                pass
                        
                        except KeyboardInterrupt:
                            print(f"invalid")
                            sockets.remove(sock) 
                            sock.close()

    except KeyboardInterrupt:
        # Close all sockets
        for sock in sockets:
            sock.close()
            

if __name__ == "__main__":
    port_num = 0
    if len(sys.argv) > 1:
        port_num = int(sys.argv[1])
        
    
    # Create UDP socket
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.bind(('0.0.0.0', port_num))
    announce(udp_socket)
    
    # Create TCP socket
    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_socket.bind(('cormorant.cs.umanitoba.ca', 15001))
    tcp_socket.listen()

    # Start listening on separate threads for UDP and TCP sockets
    udp_thread = threading.Thread(target=listen, args=(udp_socket,))

    tcp_thread = threading.Thread(target=start_cli_server, args=(tcp_socket,udp_socket))

    udp_thread.start()
    tcp_thread.start()

    try:
        udp_thread.join()
        tcp_thread.join()
    except KeyboardInterrupt:
        print("Interrupted. Closing sockets...")
        udp_socket.close()
        tcp_socket.close()

    except Exception as e:
        print("Error:", e)
    