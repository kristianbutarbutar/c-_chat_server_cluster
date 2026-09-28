#ifndef CHAT_SERVER_NODE_HPP
#define CHAT_SERVER_NODE_HPP

#include "chat_server.hpp"

class ChatServerNode {
private:
    int node_port;
    ChatServerManager server_manager;

public:
    ChatServerNode(int port, const std::string& db_conn);
    void run();
};

#endif // CHAT_SERVER_NODE_HPP