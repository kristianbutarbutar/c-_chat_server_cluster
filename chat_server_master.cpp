#include "chat_server_master.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pqxx/pqxx>
#include <random>
#include <algorithm>

ChatServerMaster::ChatServerMaster(const std::string &config_file)
    : port_c(8080), port_b(9001), port_a(7001), master_id("master_1"), master_ip("127.0.0.1"),
      routing_head(nullptr), is_active_master(false), running(true)
{

    std::ifstream file(config_file);
    std::string line;
    while (std::getline(file, line))
    {
        if (!line.empty() && line.back() == '\r')
            line.pop_back();
        if (line.empty() || line[0] == '#')
            continue;

        size_t delimiter = line.find('=');
        if (delimiter == std::string::npos)
            continue;

        std::string key = line.substr(0, delimiter);
        std::string val = line.substr(delimiter + 1);

        key.erase(remove_if(key.begin(), key.end(), ::isspace), key.end());

        size_t first = val.find_first_not_of(" \t\r\n");
        size_t last = val.find_last_not_of(" \t\r\n");
        if (first != std::string::npos && last != std::string::npos)
        {
            val = val.substr(first, (last - first + 1));
        }
        else
        {
            val = "";
        }

        if (key == "port_c")
            port_c = std::stoi(val);
        else if (key == "port_b")
            port_b = std::stoi(val);
        else if (key == "port_a")
            port_a = std::stoi(val);
        else if (key == "db_conn")
            db_connection_string = val;
        else if (key == "master_id")
            master_id = val;
        else if (key == "master_ip")
            master_ip = val;
        else if (key.rfind("node_", 0) == 0)
        {
            int nid = std::stoi(key.substr(5));
            size_t colon = val.find(':');
            if (colon != std::string::npos)
            {
                worker_nodes.push_back({nid, val.substr(0, colon), std::stoi(val.substr(colon + 1))});
            }
        }
    }

    // Intelligent Database-Backed Election
    try
    {
        pqxx::connection conn(db_connection_string);
        pqxx::work txn(conn);
        auto res = txn.exec("SELECT master_id FROM master_heartbeats WHERE status = 'ACTIVE' AND last_heartbeat >= NOW() - INTERVAL '5 minutes';");

        if (res.empty())
        {
            is_active_master = true;
            std::cout << "[MASTER ELECTION] No active master found in DB. Claiming ACTIVE role." << std::endl;
        }
        else
        {
            is_active_master = true;
            std::cout << "[MASTER ELECTION] Active master already exists in cluster. Initializing as STANDBY." << std::endl;
        }
        txn.commit();
    }
    catch (const std::exception &e)
    {
        std::cerr << "[MASTER ELECTION] DB check failed (" << e.what() << "). Falling back to random vote." << std::endl;
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(1, 10);
        int vote = dis(gen);
        is_active_master = (vote > 5);
    }

    std::cout << "[MASTER STATUS] " << master_id << " initialized as " << (is_active_master ? "IN-CHARGE (ACTIVE)" : "STANDBY") << std::endl;
}

ChatServerMaster::~ChatServerMaster()
{
    running = false;
    std::unique_lock<std::shared_mutex> lock(routing_mutex);
    RoutingNode *curr = routing_head;
    while (curr)
    {
        RoutingNode *next = curr->next;
        delete curr;
        curr = next;
    }
}

void ChatServerMaster::run()
{
    std::thread(&ChatServerMaster::heartbeatLoop, this).detach();
    std::thread(&ChatServerMaster::cleanupInactiveRoutingLoop, this).detach();
    std::thread(&ChatServerMaster::startMasterSyncListener, this).detach();

    if (is_active_master)
    {
        startClientListener();
    }
    else
    {
        while (running)
        {
            std::this_thread::sleep_for(std::chrono::seconds(30));
            try
            {
                pqxx::connection conn(db_connection_string);
                pqxx::work txn(conn);
                auto res = txn.exec("SELECT master_id FROM master_heartbeats WHERE status = 'ACTIVE' AND last_heartbeat < NOW() - INTERVAL '5 minutes';");
                if (!res.empty())
                {
                    std::cout << "[FAILOVER] Active master timeout detected! Taking over as In-Charge Master.\n";
                    txn.exec("UPDATE master_heartbeats SET status = 'ACTIVE', last_heartbeat = NOW() WHERE master_id = " + txn.quote(master_id) + ";");
                    txn.commit();
                    is_active_master = true;
                    startClientListener();
                    break;
                }
            }
            catch (const std::exception &e)
            {
                std::cerr << "[Failover Check Error]: " << e.what() << std::endl;
            }
        }
    }
}

void ChatServerMaster::heartbeatLoop()
{
    while (running)
    {
        try
        {
            pqxx::connection conn(db_connection_string);
            pqxx::work txn(conn);
            std::string query = "INSERT INTO master_heartbeats (master_id, ip_address, port, last_heartbeat, status) VALUES (" +
                                txn.quote(master_id) + ", " +
                                txn.quote(master_ip) + ", " +
                                std::to_string(port_c) + ", NOW(), " +
                                txn.quote(is_active_master ? "ACTIVE" : "STANDBY") +
                                ") ON CONFLICT (master_id) DO UPDATE SET ip_address = EXCLUDED.ip_address, port = EXCLUDED.port, last_heartbeat = NOW(), status = EXCLUDED.status;";
            txn.exec(query);
            txn.commit();
            std::cout << "[POSTGRES HEARTBEAT] Registered IP: " << master_ip << ", Port: " << port_c << " | Status: " << (is_active_master ? "ACTIVE" : "STANDBY") << std::endl;
        }
        catch (const std::exception &e)
        {
            std::cerr << "[Database Heartbeat Error]: " << e.what() << std::endl;
        }
        std::this_thread::sleep_for(std::chrono::minutes(5));
    }
}

void ChatServerMaster::cleanupInactiveRoutingLoop()
{
    while (running)
    {
        std::this_thread::sleep_for(std::chrono::minutes(60));
        if (!is_active_master)
            continue;

        std::unique_lock<std::shared_mutex> lock(routing_mutex);
        RoutingNode *curr = routing_head;
        RoutingNode *prev = nullptr;
        auto now = std::chrono::steady_clock::now();

        while (curr)
        {
            auto inactive_duration = std::chrono::duration_cast<std::chrono::minutes>(now - curr->last_chat).count();
            if (inactive_duration >= 60)
            {
                std::cout << "[ROUTING SCANNER] Dropping inactive room ID: " << curr->id << " (Inactive for " << inactive_duration << " mins)\n";

                try
                {
                    pqxx::connection conn(db_connection_string);
                    pqxx::work txn(conn);
                    txn.exec("DELETE FROM routing_mapping WHERE id = " + txn.quote(curr->id) + ";");
                    txn.commit();
                }
                catch (...)
                {
                }

                if (prev)
                {
                    prev->next = curr->next;
                    delete curr;
                    curr = prev->next;
                }
                else
                {
                    routing_head = curr->next;
                    delete curr;
                    curr = routing_head;
                }
            }
            else
            {
                prev = curr;
                curr = curr->next;
            }
        }
    }
}

int ChatServerMaster::getLowestLoadNode()
{
    static size_t idx = 0;
    if (worker_nodes.empty())
        return 1;
    int selected = worker_nodes[idx % worker_nodes.size()].node_id;
    idx++;
    return selected;
}

int ChatServerMaster::findOrCreateAssignedNode(const std::vector<std::string> &participants)
{
    std::vector<std::string> sorted_participants = participants;
    std::sort(sorted_participants.begin(), sorted_participants.end());
    sorted_participants.erase(std::unique(sorted_participants.begin(), sorted_participants.end()), sorted_participants.end());

    std::string room_key = "";
    for (size_t i = 0; i < sorted_participants.size(); ++i)
    {
        room_key += sorted_participants[i];
        if (i + 1 < sorted_participants.size())
            room_key += "_";
    }

    std::unique_lock<std::shared_mutex> lock(routing_mutex);

    RoutingNode *curr = routing_head;
    while (curr)
    {
        if (curr->id == room_key)
        {
            curr->last_chat = std::chrono::steady_clock::now();
            return curr->node_id;
        }
        curr = curr->next;
    }

    int assigned_node = getLowestLoadNode();
    auto node_endpoint = worker_nodes[0];
    for (const auto &n : worker_nodes)
    {
        if (n.node_id == assigned_node)
            node_endpoint = n;
    }

    RoutingNode *newNode = new RoutingNode(room_key, assigned_node, "2026-09-23 12:00:00", "active");
    newNode->next = routing_head;
    routing_head = newNode;

    try
    {
        pqxx::connection conn(db_connection_string);
        pqxx::work txn(conn);
        std::string query = "INSERT INTO routing_mapping (id, node_id, node_ip, node_port, timestamp, status) VALUES (" +
                            txn.quote(room_key) + ", " + std::to_string(assigned_node) + ", " +
                            txn.quote(node_endpoint.host) + ", " + std::to_string(node_endpoint.port) +
                            ", NOW(), 'active') ON CONFLICT (id) DO NOTHING;";
        txn.exec(query);
        txn.commit();
    }
    catch (const std::exception &e)
    {
        std::cerr << "[Database Routing Store Error]: " << e.what() << std::endl;
    }

    return assigned_node;
}

std::string ChatServerMaster::forwardToNode(int node_id, const std::string &payload)
{
    auto it = std::find_if(worker_nodes.begin(), worker_nodes.end(), [node_id](const NodeEndpoint &n)
                           { return n.node_id == node_id; });
    if (it == worker_nodes.end())
        return "{\"status\":\"error\",\"message\":\"Node not found\"}";

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(it->port);
    inet_pton(AF_INET, it->host.c_str(), &serv_addr.sin_addr);

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        close(sock);
        return "{\"status\":\"error\",\"message\":\"Failed to connect to worker node\"}";
    }

    send(sock, payload.c_str(), payload.length(), 0);
    char buffer[4096] = {0};
    int valread = read(sock, buffer, 4096);
    close(sock);

    return valread > 0 ? std::string(buffer, valread) : "{}";
}

void ChatServerMaster::broadcastToAllNodes(const std::string &payload)
{
    for (const auto &node : worker_nodes)
    {
        forwardToNode(node.node_id, payload);
    }
}

static std::string extractMasterJsonField(const std::string &json, const std::string &key)
{
    std::string searchKey = "\"" + key + "\":";
    size_t pos = json.find(searchKey);
    if (pos == std::string::npos)
        return "";

    size_t start = pos + searchKey.length();
    while (start < json.length() && (json[start] == ' ' || json[start] == '\t' || json[start] == '\n' || json[start] == '\r'))
    {
        start++;
    }
    if (start >= json.length())
        return "";

    if (json[start] == '"')
    {
        start++;
        size_t end = json.find("\"", start);
        if (end == std::string::npos)
            return "";
        return json.substr(start, end - start);
    }
    else
    {
        size_t end = start;
        while (end < json.length() && json[end] != ',' && json[end] != '}' && json[end] != ']')
        {
            end++;
        }
        return json.substr(start, end - start);
    }
}

static std::vector<std::string> extractMasterJsonArray(const std::string &json, const std::string &key)
{
    std::vector<std::string> list;
    std::string searchKey = "\"" + key + "\":";
    size_t pos = json.find(searchKey);
    if (pos == std::string::npos)
        return list;

    size_t bracketStart = json.find("[", pos);
    size_t bracketEnd = json.find("]", pos);
    if (bracketStart == std::string::npos || bracketEnd == std::string::npos)
        return list;

    std::string arrayContent = json.substr(bracketStart + 1, bracketEnd - bracketStart - 1);
    std::stringstream ss(arrayContent);
    std::string item;
    while (std::getline(ss, item, ','))
    {
        size_t first = item.find_first_not_of(" \t\"");
        size_t last = item.find_last_not_of(" \t\"");
        if (first != std::string::npos && last != std::string::npos)
        {
            list.push_back(item.substr(first, (last - first + 1)));
        }
    }
    return list;
}

std::string ChatServerMaster::handleClientRequest(const std::string &request)
{
    if (request.find("\"action\":\"add_user\"") != std::string::npos)
    {
        broadcastToAllNodes(request);
        return "{\"status\":\"success\",\"message\":\"User successfully broadcasted to all node instances\"}";
    }

    if (request.find("\"action\":\"start_voice_call\"") != std::string::npos)
    {
        std::string uid = extractMasterJsonField(request, "uid");
        std::vector<std::string> touids = extractMasterJsonArray(request, "touid");

        std::vector<std::string> participants = touids;
        if (std::find(participants.begin(), participants.end(), uid) == participants.end())
        {
            participants.push_back(uid);
        }

        std::sort(participants.begin(), participants.end());
        std::string room_key = "";
        for (size_t i = 0; i < participants.size(); ++i)
        {
            room_key += participants[i];
            if (i + 1 < participants.size())
                room_key += "_";
        }

        std::cout << "[VOICE ROUTING] Assigning voice room " << room_key << " to Voice Node port 9010" << std::endl;
        return "{\"status\":\"success\",\"room_id\":\"" + room_key + "\",\"voice_host\":\"127.0.0.1\",\"voice_port\":9010}";
    }

    std::string uid = extractMasterJsonField(request, "uid");
    std::vector<std::string> touids = extractMasterJsonArray(request, "touid");

    if (uid.empty())
    {
        return "{\"status\":\"error\",\"message\":\"Missing UID for routing lookup\"}";
    }

    std::vector<std::string> participants = touids;
    if (std::find(participants.begin(), participants.end(), uid) == participants.end())
    {
        participants.push_back(uid);
    }

    int target_node = findOrCreateAssignedNode(participants);
    std::cout << "[MASTER ROUTING] Forwarding request to Worker Node " << target_node << std::endl;
    return forwardToNode(target_node, request);
}

void ChatServerMaster::startClientListener()
{
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port_c);

    bind(server_fd, (struct sockaddr *)&address, sizeof(address));
    listen(server_fd, 20);

    std::cout << "[MASTER PORT C] Active Master listening for multi-client requests on port " << port_c << std::endl;
    while (is_active_master && running)
    {
        int new_socket = accept(server_fd, nullptr, nullptr);
        if (new_socket < 0)
            continue;

        std::thread([this, new_socket]()
                    {
            char buffer[8192] = {0};
            int valread = read(new_socket, buffer, 8192);
            if (valread > 0) {
                std::string response = handleClientRequest(std::string(buffer, valread));
                send(new_socket, response.c_str(), response.length(), 0);
            }
            close(new_socket); })
            .detach();
    }
    close(server_fd);
}

void ChatServerMaster::startMasterSyncListener()
{
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port_a);

    bind(server_fd, (struct sockaddr *)&address, sizeof(address));
    listen(server_fd, 5);

    while (running)
    {
        int sock = accept(server_fd, nullptr, nullptr);
        if (sock < 0)
            continue;
        char buffer[1024] = {0};
        read(sock, buffer, 1024);
        std::string ack = "SYNC_ACK";
        send(sock, ack.c_str(), ack.length(), 0);
        close(sock);
    }
    close(server_fd);
}