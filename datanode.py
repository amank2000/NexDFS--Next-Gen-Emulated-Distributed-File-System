import os
import sys
import socket
import json
import base64

class DataNode:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.blocks_available = 128
        self.storage_path = os.getcwd()+"/"+str(port)

    # start listening on datanode port
    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.port))
            s.listen()
            print(f"DataNode started at {self.ip}:{self.port}")
            try:
                while True:
                    conn, addr = s.accept()
                    print(f"Connection from {addr}")
                    self.handle_command(conn)
            except KeyboardInterrupt:
                s.close()
                print("\nProgram terminated by user.")
    
    # receive command
    def handle_command(self, conn):
        with conn:
            data = conn.recv(4096)
            command = json.loads(data.decode())
            response = self.process_command(command)
            conn.sendall(json.dumps(response).encode())

    # process command
    def process_command(self, command):
        if command["command"]=="put":
            return self.write_new_file(command["block_id"], command["data"]) 
        if command["command"]=="rm":
            return self.remove_file(command["block_id"]) 
        if command["command"]=="get":
            return self.get_file_content(command["block_id"])
        if command["command"]=="cat":
            return self.get_file_content(command["block_id"])
        if command["command"]=="block_content":
            return self.get_file_content(command["block_id"])


    def write_new_file(self, block_id, data):
        data = base64.b64decode(data)
        local_file_path = self.storage_path+"/"+str(block_id)
        with open(local_file_path, "wb") as f:
            f.write(data)
        return {"command":"put", "status":"success","message":"Successfully written on DataNode "+str(self.port)}
    
    def get_file_content(self, block_id):
        local_file_path = self.storage_path+"/"+str(block_id)
        try:
            with open(local_file_path, "rb") as f:
                block = f.read()
            # base64 encoding required to send file over socket
            block_b64 = base64.b64encode(block).decode() 
            return {"command":"get", "status":"success","block":block_b64}
        except:
            return {"command":"get", "status":"error","message":"Block not found on datanode"+str(self.port)}
    
    def remove_file(self, block_id):
        local_file_path = self.storage_path+"/"+str(block_id)
        try:
            os.remove(local_file_path) 
            print(f"File '{local_file_path}' deleted successfully.")
            return {"command":"rm", "status":"success","message":"Deleted on DataNode "+str(self.port)}
        except OSError as e:
            {"command":"rm", "status":"error", "message":f"Failed to delete file '{local_file_path}': {e}"}


if __name__ == "__main__":

    if len(sys.argv)!=3:
        print("Correct usage: python3 datanode.py <datanode_ip> <datanode_port>\n")
        sys.exit(1)
    datanode_ip = sys.argv[1]
    datanode_port = int(sys.argv[2])
    
    datanode = DataNode(datanode_ip, datanode_port)
    datanode.start()