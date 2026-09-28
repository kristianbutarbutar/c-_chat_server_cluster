#ifndef CHAT_SERVER_MASTER_HPP
#define CHAT_SERVER_MASTER_HPP

#include <string>
#include <vector>
#include <shared_mutex>
#include <thread>
#include <atomic>
#include <chrono>

struct RoutingNode {
    std::string id;             // uid or touid member
    int node_id;                // assigned node number
    std::string timestamp;
    std::string status;
    std::chrono::steady_clock::time_point last_chat;
    RoutingNode* next;

    RoutingNode(const std::string& i, int n, const std::string& ts, const std::string& stat)
        : id(i), node_id(n), timestamp(ts), status(stat), last_chat(std::chrono::steady_clock::now()), next(nullptr) {}
};

struct NodeEndpoint {
    int node_id;
    std::string host;
    int port;
};

class ChatServerMaster {
private:
    // Ordered to match ChatServerMaster constructor initialization list exactly
    int port_c; 
    int port_b; 
    int port_a; 
    std::string master_id;
    std::string master_ip;
    std::string db_connection_string;
    
    std::vector<NodeEndpoint> worker_nodes;
    
    RoutingNode* routing_head;
    mutable std::shared_mutex routing_mutex;

    std::atomic<bool> is_active_master;
    std::atomic<bool> running;

    void startClientListener();
    void startMasterSyncListener();
    void heartbeatLoop();
    void cleanupInactiveRoutingLoop();

    int findOrCreateAssignedNode(const std::vector<std::string>& participants);
    int getLowestLoadNode();
    std::string forwardToNode(int node_id, const std::string& payload);
    void broadcastToAllNodes(const std::string& payload);

    std::string handleClientRequest(const std::string& request);

public:
    ChatServerMaster(const std::string& config_file);
    ~ChatServerMaster();
    void run();
};

#endif // CHAT_SERVER_MASTER_HPP