
# Emulated Distributed File System (EDFS) 📂

EDFS is a robust emulation of the renowned Hadoop Distributed File System (HDFS). Our aim is to bring the power and functionality of HDFS to a compact, yet effective, emulation. Dive into a system that mirrors HDFS's architecture with a metadata server (Namenode), multiple data servers (Datanodes), and a user-friendly EDFS client.

---

## 🌟 Features:

- 🖥️ **EDFS Client**: Dive into an interactive client that seamlessly handles shell commands.
- 📚 **Namenode**: Your trusted manager for all file system metadata.
- 💾 **Datanodes**: Reliable data storage solutions that focus on redundancy and data integrity.
- 🌐 **Web Interface**: A sleek, web-based UI for a hands-on experience with EDFS.

---

## 🔧 Technical Highlights:

- **Centralized Metadata Management**: Efficiently handle vast amounts of file system info with our dedicated Metadata Server.
- **Adaptive File Handling**: Large file? No problem. Our system splits it across multiple Datanodes.
- **Seamless Communication**: Our DFS client is built for uninterrupted communication with the metadata server.
- **Maximized Data Reliability**: Sleep easy with our top-tier replication and failure-handling mechanisms.
- **Explore with Ease**: Our interactive Web UI is intuitive and crafted for optimal user experience.

---

## 🚀 Getting Started:

**File Structure Explanation:**
Confirm 3 empty folders with names 9002, 9003 and 9004 are present in the current working directory.

**Port Numbers:**
Client: 9000
Namenode: 9001
Datanode 1: 9002
Datanode 2: 9003
Datanode 3: 9004
IP: localhost (127.0.0.1:5000)

**To Run:**
Terminal 1 (Client): python3 client.py localhost 9000 9001
Terminal 2 (NameNode): python3 namenode.py localhost 9001 9002 9003 9004 Terminal 3 (DataNode 1): python3 datanode.py localhost 9002 9001
Terminal 4 (DataNode 2): python3 datanode.py localhost 9003 9001
Terminal 5 (DataNode 3): python3 datanode.py localhost 9004 9001
Terminal 6 (Flask UI): python3 app.py


---

## 📜 License:

Licensed under the [MIT License](link_to_license). Feel free to use, modify, and distribute as per the license's terms.
