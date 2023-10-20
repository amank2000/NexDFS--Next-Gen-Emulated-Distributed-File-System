import os
from flask import Flask, render_template, request, flash, redirect, url_for
import json
import client
import namenode
import datanode

ui_client = client.Client("localhost",9000,9001)
# ui_client.run()
ui_namenode = namenode.NameNode("localhost",9001,[9002,9003,9004])
# ui_namenode.start()
ui_datanodes = []
for port in [9002,9003,9004]:
    ui_datanodes.append(datanode.DataNode("localhost",port))
# for dn in ui_datanodes:
#     dn.start()

app = Flask(__name__)
app.secret_key = '21jsxo3n'

def is_folder(item, metadata):
    return item in metadata["dir"].keys()

def get_separate_files_and_folders(metadata,parent,current_directory):
    folders, files = [], []
    if parent=='':  # root
        children = metadata["dir"][current_directory]["children"]  
        for child in children:
            if is_folder(parent+current_directory+child,metadata):
                folders.append(child)
            else:
                files.append(child)
        return folders,files  
    if parent=='/':    # dirs under root
        children = metadata["dir"][parent+current_directory]["children"]  
        for child in children:
            if is_folder(parent+current_directory+"/"+child,metadata):
                folders.append(child)
            else:
                files.append(child)
        return folders,files 
    else:   # all other dirs
        children = metadata["dir"][parent+current_directory]["children"]    
        for child in children:
            if is_folder(parent+current_directory+"/"+child,metadata):
                folders.append(child)
            else:
                files.append(child)
        return folders,files

# Route for home page
@app.route('/', methods=['GET','POST'])
def home():
    directory = '/'  
    with open("metadata.json","r") as f:
        metadata = json.load(f)
    folders, files = get_separate_files_and_folders(metadata,'',directory)

    return render_template('index.html', directory=directory, folders=folders, files=files)

# Display contents of a directory (command: ls)
@app.route('/folder/<parent>/<folder_name>', methods=['POST','GET'])
def folder(parent, folder_name):
    parent = parent.replace('-','/')
    if parent=="root":
        parent = "/"
    else:
        parent = "/"+parent.replace("root/","")+"/"
    
    current_directory = folder_name
    with open("metadata.json","r") as f:
        metadata = json.load(f)
    folders, files = get_separate_files_and_folders(metadata,parent,current_directory)
    return render_template('directory.html', directory=parent+current_directory, folders=folders, files=files)

# Display file contents (command: cat)
@app.route('/file/<parent>/<file_name>', methods=['POST','GET'])
def file(parent,file_name):
    local_path = str(os.getcwd())+"/"+file_name
    parent = parent.replace('-','/')
    if parent=="root":
        parent = "/"
    else:
        parent = "/"+parent.replace("root/","")+"/"
    content = str(ui_client.execute_command("cat "+parent+file_name))
    content = content.replace('\n', '<br>')
    return render_template('display_file.html',path=parent+file_name,file_name=file_name, content=content,dic=local_path)

# Upload a file (command: put)
@app.route('/upload', methods=['POST'])
def upload():
    parent = request.form.get('path')
    parent = parent.replace('-','/')
    if parent=="root":
        parent = "/"
    else:
        parent = "/"+parent.replace("root/","")+"/"

    files = request.files.getlist('file[]')
    for file in files:
        edfs_path = parent+file.filename
        file_content = file.read()

        file_name = file.filename[:file.filename.rfind(".")]
        file_ext = file.filename[file.filename.rfind(".")+1:]
        temp_file = file_name+"_temp."+file_ext

        with open(temp_file, 'wb') as f:
            f.write(file_content)
        ui_client.execute_command("put "+temp_file+" "+edfs_path)
        os.remove(temp_file)

    if parent=="/":
        return redirect(url_for("home"))
    parent = ("root/"+parent[1:-1]).replace("/","-")
    return redirect(url_for("folder", parent=parent[:parent.rfind("-")], folder_name=parent[parent.rfind("-")+1:]))

    
# Download a file (command: get)
@app.route('/download',methods=['POST'])
def download():
    edfs_path = request.form.get('parent').replace('-','/')
    file_name = request.form.get('file_name')
    local_path = str(os.getcwd())+"/"+file_name
    ui_client.execute_command("get "+edfs_path+" "+local_path)
    parent = "root" if edfs_path[:edfs_path.rfind("/")]=="" else ("root"+edfs_path[:edfs_path.rfind("/")]).replace("/","-")
    
    return redirect(url_for("file", parent=parent, file_name=file_name,dic=local_path))    

# View file blocks
@app.route('/blocks',methods=['POST'])
def blocks():
    edfs_path = request.form.get('parent').replace('-','/')
    file_name = request.form.get('file_name')
    blocks_metadata = ui_client.get_blocks_metadata(edfs_path)
    
    content = "<hr>\n\n"
    for b in blocks_metadata:
        content = content + "Partition: " + str(b["partition"]) + "\nSize: " + str(b["num_bytes"]) + "\nDatanode: " + str(b["datanode"]) + "\nBlock ID: " + b["id"] + "\n\nBlock Content:\n\n"
        content = content +ui_client.get_block_content(b["id"],b["datanode"])
        content = content + "\n\n<hr>\n\n"
    content = content.replace('\n', '<br>')
    
    return render_template('display_blocks.html',path=edfs_path,file_name=file_name, content=content)


if __name__ == '__main__':
    app.run(debug=True)
