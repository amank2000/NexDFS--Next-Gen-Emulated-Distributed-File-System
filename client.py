import base64
import json
import os
import shutil
import socket
import sys

class Client:
    def __init__(self, ip, port, namenode_port): 
        self.ip = ip
        self.port = port
        self.namenode = namenode_port

    # get user input commands
    def run(self):
        while True:
            try:
                command = input("EDFS> ")
                if command == "exit":
                    print("Bye!")
                    break
                self.execute_command(command)
            except Exception as e:
                print(f"Error: {e}")
            
    # execute user input commands
    def execute_command(self, command):
        cmd_parts = command.split()
        cmd = cmd_parts[0]
        if cmd == "ls":     
            return self.ls(cmd_parts[1] if len(cmd_parts) > 1 else "/")
        elif cmd == "rm" and len(cmd_parts)==2:   
            return self.rm(cmd_parts[1])
        elif cmd == "put" and len(cmd_parts)==3:  
            return self.put(cmd_parts[1], cmd_parts[2])
        elif cmd == "get" and len(cmd_parts)==3:
            return self.get(cmd_parts[1], cmd_parts[2])
        elif cmd == "mkdir" and len(cmd_parts)==2:    
            return self.mkdir(cmd_parts[1])
        elif cmd == "rmdir" and len(cmd_parts)==2:    
            return self.rmdir(cmd_parts[1])
        elif cmd == "cat" and len(cmd_parts)==2:
            return self.cat(cmd_parts[1])
        elif cmd == "blocks_metadata" and len(cmd_parts)==2:
            return self.get_blocks_metadata(cmd_parts[1])
        elif cmd == "block_content" and len(cmd_parts)==3:
            return self.get_block_content(cmd_parts[1],int(cmd_parts[2]))
        else:
            print("Invalid command/Invalid use of command:", cmd)

    # send commands to namenode over socket
    def send_to_namenode(self, command):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(("localhost",self.namenode))
                s.sendall(json.dumps(command).encode())
                response = s.recv(4096)
                return json.loads(response)
        except Exception as e:
            print(f"Error sending command to NameNode: {e}")
            return {"status": "error", "message": str(e)}
        
    # send commands to datanode over socket
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
        
    # split a file into partitions by size
    def split_file(self, src_file_path, src_file_name, partition_size, splits_dir):
        with open(src_file_path, 'r') as f:
            file_contents = f.read()
        num_partitions = (len(file_contents) + partition_size - 1) // partition_size

        for i in range(num_partitions):
            # slice the file contents based on the indices
            start_index = i * partition_size
            end_index = (i + 1) * partition_size
            partition_contents = file_contents[start_index:end_index]
            # write 
            partition_path = splits_dir+"/"+src_file_name[:src_file_name.index(".")]+"_"+str(i+1)+"."+src_file_name[src_file_name.index(".")+1:]
            with open(partition_path, 'w') as partition_file:
                partition_file.write(partition_contents)


    # command put - upload file from local machine to edfs 
    def put(self, src, dst):
        # get size of file in bytes
        file_size = os.stat(src).st_size  
        # send a write request to namenode
        namenode_response = self.send_to_namenode({"command":"put", "file_path":dst, "file_size":file_size})
        if namenode_response["status"]=="error":
            print(namenode_response["message"])
            return 0
        
        # namenode returns locations to store the file 
        locations = namenode_response["locations"]      

        # namenode also returns the default EDFS block size to help splitting
        bytes_per_split = namenode_response["block_size"]
        print("\nFile size:",file_size)
        print("Allowed block size:",namenode_response["block_size"])

        block_sizes = {}    # to record the actual sie of each block

        if len(locations)==1:   # no file splitting required
            block_sizes[0] = file_size
            with open(src, "rb") as f:
                file = f.read()
            # base64 encoding required to send file over socket
            file_b64 = base64.b64encode(file).decode()  
                
            # sending file to datanode
            r = 1
            for replica in locations[0]:
                print("\nSending replica",str(r),"to DataNode port",replica[0])
                response_dn = self.send_to_datanode({"command":"put", "block_id":replica[1], "data":file_b64},replica[0])    
                print(response_dn["message"])
                if response_dn["status"]=="error":
                    print(response_dn["message"])
                    return 0
                r+=1

        elif len(locations)>1:
            print("Splitting file into",len(namenode_response["locations"]),"partitions")

            splits_dir = os.getcwd()+"/splits"
            if os.path.exists(splits_dir):
                shutil.rmtree(splits_dir)
            os.mkdir(splits_dir)

            # split the file by block size
            src_file_name = src[src.rfind("/")+1:]
            self.split_file(src, src_file_name, bytes_per_split, splits_dir)
            
            for filename in os.listdir(splits_dir):
                split_num = int(filename[:filename.index(".")].split('_')[-1])-1
                if split_num not in block_sizes.keys():
                    block_sizes[split_num] = os.stat(splits_dir+"/"+filename).st_size

                # file encoding
                with open(splits_dir+"/"+filename, "rb") as f:
                    file = f.read()
                file_b64 = base64.b64encode(file).decode()
                
                # sending file split to datanode
                r = 1
                for replica in locations[split_num]:
                    print("\nSending partition",str(split_num+1),"replica",str(r),"to DataNode port",replica[0])
                    response_dn = self.send_to_datanode({"command":"put", "block_id":replica[1], "data":file_b64},replica[0])    
                    if response_dn["status"]=="error":
                        print(response_dn["message"])
                        return 0
                    r+=1

            shutil.rmtree(splits_dir)

        # telling the namenode to update metadata for the newly saved file
        response_new_file = self.send_to_namenode({"command":"put_update", "file_path":dst, "file_size":file_size, "locations":locations, "block_sizes":block_sizes})
        print(response_new_file["message"])
        return 1
    
    # command ls - list contents of a directory
    def ls(self, path):
        namenode_response = self.send_to_namenode({"command":"ls", "path":path})
        if namenode_response["status"]=="error":
            print(namenode_response["message"])
            return 0
        elif namenode_response["status"]=="success":
            print()
            for item in namenode_response["list"]:
                print(item)
            print()
            return 1
        
    # command rm - delete a file
    def rm(self, path):
        namenode_response = self.send_to_namenode({"command":"rm", "path":path})
        print(namenode_response["message"])
        return 0 if namenode_response["status"]=="error" else 1
    
    # command mkdir - create a directory
    def mkdir(self, path):
        namenode_response = self.send_to_namenode({"command":"mkdir", "path":path})
        print(namenode_response["message"])
        return 0 if namenode_response["status"]=="error" else 1

    # command rmdir - delete a directory
    def rmdir(self, path):
        namenode_response = self.send_to_namenode({"command":"rmdir", "path":path})
        print(namenode_response["message"])
        return 0 if namenode_response["status"]=="error" else 1

    # command get - download file from edfs to local machine
    def get(self, file_path, local_path):
        namenode_response = self.send_to_namenode({"command":"get", "file_path":file_path})
        if namenode_response["status"]=="error":
            print(namenode_response["message"])
            return 0

        partitions = {}
        for block in namenode_response["blocks"]:
            p = block["partition"]
            if p not in partitions.keys():
                partitions[p] = []
            partitions[p].append([block["id"], block["datanode"]])

        splits_dir = os.getcwd()+"/splits"
        if os.path.exists(splits_dir):
            shutil.rmtree(splits_dir)
        os.mkdir(splits_dir)

        file_content = ""

        for p in partitions.keys():
            for replica in partitions[p]:
                try:
                    block_id = replica[0]
                    datanode_port = replica[1]
                    datanode_response = self.send_to_datanode({"command":"get", "block_id":block_id},datanode_port)
                    data = base64.b64decode(datanode_response["block"]).decode()
                    file_content+=data
                    break
                except:
                    continue

        with open(local_path, 'w') as merged_file:
            merged_file.write(file_content)

        print("File saved to",local_path)
        shutil.rmtree(splits_dir)

    # command cat - display file contents on the terminal
    def cat(self, file_path):
        namenode_response = self.send_to_namenode({"command":"cat", "file_path":file_path})
        if namenode_response["status"] == "error":
            print(namenode_response["message"])
            return 0

        partitions = {}
        for block in namenode_response["blocks"]:
            p = block["partition"]
            if p not in partitions.keys():
                partitions[p] = []
            partitions[p].append([block["id"], block["datanode"]])

        splits_dir = os.getcwd()+"/splits"
        if os.path.exists(splits_dir):
            shutil.rmtree(splits_dir)
        os.mkdir(splits_dir)

        file_content = ""
        for p in partitions.keys():
            for replica in partitions[p]:
                try:
                    block_id = replica[0]
                    datanode_port = replica[1]
                    datanode_response = self.send_to_datanode({"command":"cat", "block_id":block_id},datanode_port)
                    data = base64.b64decode(datanode_response["block"]).decode()
                    file_content+=data
                    break
                except:
                    continue

        print(file_content)
        shutil.rmtree(splits_dir)
        return file_content

    # for client UI
    def get_blocks_metadata(self, file_path):
        namenode_response = self.send_to_namenode({"command":"blocks_metadata", "file_path":file_path})
        print(namenode_response)
        return eval(namenode_response['data'])
    
    # for client UI
    def get_block_content(self, block_id, datanode_port):
        datanode_response = self.send_to_datanode({"command":"block_content","block_id":block_id},datanode_port)
        print(type(base64.b64decode(datanode_response["block"]).decode()))
        return str(base64.b64decode(datanode_response["block"]).decode())

if __name__ == "__main__":
    if len(sys.argv)!=4:
        print("Correct usage: python3 client.py <client_ip> <client_port> <namenode_port>\n") 
        sys.exit(1)
    client_ip = sys.argv[1]
    client_port = int(sys.argv[2])
    namenode_port = int(sys.argv[3])

    client = Client(client_ip, client_port, namenode_port)
    client.run()