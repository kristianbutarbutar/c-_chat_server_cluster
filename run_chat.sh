./chat_server_cluster node 9001 config_node/config_9001.cfg &
./chat_server_cluster node 9002 config_node/config_9002.cfg &
./chat_server_cluster node 9003 config_node/config_9003.cfg &

./chat_server_cluster master config_master/config.cfg &
