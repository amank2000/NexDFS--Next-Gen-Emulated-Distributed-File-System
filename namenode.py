import socket
import json
import sys
import random
import uuid
import socket

class NameNode:
    def __init__(self, ip, port, datanode_ports):
        self.ip = ip
        self.port = port
        self.datanodes = datanode_ports

        with open("metadata.json","r") as f:
            metadata = json.load(f)            
        
        self.file_metadata = metadata["file"]   
        self.directory_metadata = metadata["dir"]
        
        self.block_size = 2048

    # start listening on namenode port
    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.port))
            s.listen()
            print(f"NameNode started at {self.ip}:{self.port}")
            try:
                while True:
                    conn, addr = s.accept()
                    print(f"Connection from {addr}")
                    self.handle_client(conn)

            except KeyboardInterrupt:
                s.close()
                print("\nProgram terminated by user.")
        
    # receive command from client
    def handle_client(self, conn):
        with conn:
            data = conn.recv(4096)
            command = json.loads(data)
            response = self.process_command(command)
            conn.sendall(json.dumps(response).encode())

    # process command
    def process_command(self, command):
        if command["command"]=="put":
            return self.write_new_file(command["file_path"], command["file_size"]) 
        if command["command"]=="put_update":
            return self.write_new_file_update_metadata(command["file_path"],command["file_size"],command["locations"],command["block_sizes"])
        if command["command"]=="ls":
            return self.ls(command["path"])
        if command["command"]=="rm":
            return self.remove_file(command["path"])
        if command["command"]=="mkdir":
            return self.make_directory(command["path"])
        if command["command"]=="rmdir":
            return self.remove_directory(command["path"])
        if command["command"]=="get":
            return self.get_block_locations(command["file_path"])
        if command["command"]=="cat":
            return self.get_block_locations(command["file_path"])
        if command["command"]=="blocks_metadata":
            return self.blocks_metadata(command["file_path"])
        
    # send command to datanode
    def send_to_datanode(self, command, dn_port):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(("localhost",dn_port))
                s.sendall(json.dumps(command).encode())              
                response = s.recv(4096)
                return json.loads(response)
        except Exception as e:
            print(f"Error sending command to DataNode: {e}")
            return {"status": "error", "message": str(e)}
            
    # create a new file
    def write_new_file(self, file_path, file_size):
        if "." not in file_path:
            return {"command":"put", "status":"error", "message":"Namenode Error: Invalid file name"}

        # check if file already exists in metadata
        if file_path in self.file_metadata.keys():
            return {"command":"put", "status":"error", "message":"Namenode Error: File already exists"}
        
        # check if parent directory exists
        parent_path = file_path[:file_path.rfind('/')]
        if len(parent_path)>1 and parent_path not in self.directory_metadata.keys():
            return {"command":"put", "status":"error", "message":"Namenode Error: Parent directory does not exist"}

        locations = []
        
        # calculate number of partitions to split the file into
        n_partitions = 1    # default number of partitions is 1
        if file_size > self.block_size:
            n_partitions = (file_size + self.block_size - 1) // self.block_size

        # select 2 datanodes at random (since rf=2) to send each partition to
        for p in range(n_partitions):
            dn = random.sample(range(3), 2)
            dn_with_blockid = [(self.datanodes[i],str(uuid.uuid4())) for i in dn]   # uuid creates a unique block_id for each replica
            
            locations.append(dn_with_blockid) 
        
        # return locations and block size to client
        return {"command":"put", "locations":locations, "block_size":self.block_size, "status":"success",}

    # updates metadata
    def write_new_file_update_metadata(self, file_path, file_size, locations, block_sizes):
        self.file_metadata[file_path] = {
            "rf": 2,
            "size": file_size,  
            "blocks": []
        }
        p = 0
        for partition in locations:
            block_size = block_sizes[str(p)]
            p+=1
            for replica in partition:
                self.file_metadata[file_path]["blocks"].append({
                    "id":replica[1],
                    "partition": p,
                    "datanode":replica[0],
                    "num_bytes": block_size
                })
        
        parent_path = file_path[:file_path.rfind('/')]
        if parent_path=="":
            parent_path="/"
        file_name = file_path[file_path.rfind('/')+1:]
        self.directory_metadata[parent_path]["children"].append(file_name)

        self.save_metadata()    # saving metadata.json
        return {"command":"put_update", "status":"success", "message":"File uploaded successfully"}
    
    def save_metadata(self):
        data = {"file":self.file_metadata, "dir":self.directory_metadata}
        with open("metadata.json","w") as f:
            json.dump(data,f)

    # list contents of directory
    def ls(self, path):
        if len(path)>1 and path[-1]=="/":
            path = path[:-1]
        if path in self.directory_metadata.keys():
            return {"command":"ls", "list": self.directory_metadata[path]["children"], "status": "success"}
        else:
            return {"command":"ls", "status":"error", "message":"Namenode Error: Path does not exist"}
        
    # delete a file
    def remove_file(self, path):
        parent_path = path[:path.rfind('/')]
        if parent_path=="":
            parent_path="/"
        file_name = path[path.rfind('/')+1:]
        if '.' not in file_name:
            return {"command":"rm", "status":"error", "message":"Namenode Error: Input isn't a file"}
        if path not in self.file_metadata.keys():
            return {"command":"rm", "status":"error", "message":"Namenode Error: File doesn't exist"}
    
        # delete file
        for block in self.file_metadata[path]["blocks"]:
            dn_port = block["datanode"]
            block_id = block["id"]
            datanode_response = self.send_to_datanode({"command": "rm", "block_id":block_id},dn_port)
       
        # update metadata
        self.file_metadata.pop(path)
        self.directory_metadata[parent_path]["children"].remove(file_name)
        self.save_metadata()    # saving metadata.json
        return {"command":"rm", "status":"success", "message":"File deleted"}
    
    # create a new directory 
    def make_directory(self, path):
        if path[0]!="/":
            return {"command":"mkdir", "status":"error", "message":"Namenode Error: Invalid path"}
        if len(path)>1 and path[path.rfind("/")+1:].isalnum()==0:
            return {"command":"mkdir", "status":"error", "message":"Namenode Error: Invalid path"}
        if path in self.directory_metadata.keys():
            if path=="/":
                return {"command":"mkdir", "status":"error", "message":"Namenode Error: Cannot create root directory, already exists"}    
            return {"command":"mkdir", "status":"error", "message":"Namenode Error: Directory already exists"}

        # check if parent directory exists
        parent_path = path[:path.rfind('/')]
        if parent_path=="":
            parent_path = "/"
        if parent_path not in self.directory_metadata.keys():
            return {"command":"mkdir", "status":"error", "message":"Namenode Error: Parent directory does not exist"}
        
        children = []
        dir_name = path[path.rfind('/')+1:]
        self.directory_metadata[parent_path]["children"].append(dir_name)

        a = {"parent": parent_path, "children": children}
        if parent_path=="/":
            self.directory_metadata[parent_path + dir_name] = a
        else:
            self.directory_metadata[parent_path + "/" + dir_name] = a

        self.save_metadata()    # saving metadata.json
        return {"command":"mkdir", "status":"success", "message":"Directory Created"}

    # remove a directory 
    def remove_directory(self, path):
        parent_path = path[:path.rfind('/')]
        if parent_path=="":
            parent_path = "/"
        dir_name = path[path.rfind('/')+1:]

        if path not in self.directory_metadata.keys():
            return {"command":"rmdir", "status":"error", "message":"Namenode Error: Directory doesn't exist"}
        if len(self.directory_metadata[path]["children"]) != 0:
            return {"command":"rmdir", "status":"error", "message":"Namenode Error: Directory isn't empty"}
        if path == '/':
            return {"command":"rmdir", "status":"error", "message":"Namenode Error: Root directory cannot be deleted"}
        
        self.directory_metadata[parent_path]["children"].remove(dir_name)
        self.directory_metadata.pop(path)
        self.save_metadata()    # saving metadata.json
        return {"command":"rmdir", "status":"success", "message":"Directory Deleted"}

    # # get block locations 
    # def get_file(self, file_path):
    #     if '.' not in file_path:
    #         return {"command":"get", "status":"error", "message":"Namenode Error: Invalid file name"} 
    #     if file_path not in self.file_metadata.keys():
    #         return {"command":"get", "status":"error", "message":"Namenode Error: File doesn't exist"} 
    #     return {"command":"get", "status":"success", "rf":self.file_metadata[file_path]["rf"], "blocks":self.file_metadata[file_path]["blocks"]}

    # # get block locations
    # def cat(self, file_path):
    #     if '.' not in file_path:
    #         return {"command":"cat", "status":"error", "message":"Namenode Error: Invalid file name"}
    #     if file_path not in self.file_metadata.keys():
    #         return {"command":"cat", "status":"error", "message":"Namenode Error: File doesn't exist"} 
    #     return {"command":"cat", "status":"success", "rf":self.file_metadata[file_path]["rf"], "blocks":self.file_metadata[file_path]["blocks"]}
    
    # get block locations for client UI
    def blocks_metadata(self, file_path):
        return {"command":"blocks_metadata", "status":"success", "data": str(self.file_metadata[file_path]["blocks"])}
    
    def get_block_locations(self, file_path):
        if '.' not in file_path:
            return {"command":"block_locations", "status":"error", "message":"Namenode Error: Invalid file name"}
        if file_path not in self.file_metadata.keys():
            return {"command":"block_locations", "status":"error", "message":"Namenode Error: File doesn't exist"} 
        return {"command":"block_locations", "status":"success", "rf":self.file_metadata[file_path]["rf"], "blocks":self.file_metadata[file_path]["blocks"]}
        

if __name__ == "__main__":
    if len(sys.argv)!=6:
        print("Correct usage: python3 namenode.py <namenode_ip> <namenode_port> <datanode_port1> <datanode_port2> <datanode_port3>\n")
        sys.exit(1)
    namenode_ip = sys.argv[1]
    namenode_port = int(sys.argv[2])
    datanode_ports = []
    for i in range(3):
        datanode_ports.append(int(sys.argv[3+i]))
    
    namenode = NameNode(namenode_ip, namenode_port, datanode_ports)
    namenode.start()