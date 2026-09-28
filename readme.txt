# Start a worker node on port 9001
./chat_server_cluster node 9001 config_node/config_9001.cfg

# Start the master controller instance 1
./chat_server_cluster master config_master/config.cfg

./chat_server_cluster voice 9010


DELETE FROM master_heartbeats;

Master Config Explanation:

# --- Master Controller Configuration ---
master_id = master_instance_1
master_ip = 127.0.0.1

# Communication Ports
port_c = 8080    # Port for incoming client/requester connections
port_b = 9001    # Port for communicating with chat server worker nodes
port_a = 7001    # Port for master-to-master synchronization and election

# PostgreSQL Database Connection
db_conn = dbname=amgreatdb user=amgreat password=amgreat host=localhost port=5432

# --- Worker Node Endpoints (Managed by Master Routing) ---
node_1 = 127.0.0.1:9001
node_2 = 127.0.0.1:9002
node_3 = 127.0.0.1:9003


load chat users:
http://localhost:3003/api/loadChatUser
