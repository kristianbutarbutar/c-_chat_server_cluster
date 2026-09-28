#include "chat_server_voice.hpp"
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <thread>
#include <algorithm>

ChatServerVoiceNode::ChatServerVoiceNode(int port) : voice_port(port), running(true) {}

ChatServerVoiceNode::~ChatServerVoiceNode()
{
    running = false;
    std::unique_lock<std::shared_mutex> lock(global_rooms_mutex);
    for (auto &pair : rooms)
    {
        std::unique_lock<std::shared_mutex> room_lock(pair.second->room_mutex);
        for (auto &peer_pair : pair.second->peers)
        {
            close(peer_pair.second->socket_fd);
        }
        pair.second->peers.clear();
    }
    rooms.clear();
}

std::shared_ptr<VoiceRoom> ChatServerVoiceNode::findOrCreateRoom(const std::string &room_id)
{
    // Try shared lock first for fast-path lookup if room already exists
    {
        std::shared_lock<std::shared_mutex> lock(global_rooms_mutex);
        auto it = rooms.find(room_id);
        if (it != rooms.end()) {
            return it->second;
        }
    }

    // Acquire exclusive lock to insert a new room safely
    std::unique_lock<std::shared_mutex> lock(global_rooms_mutex);
    auto it = rooms.find(room_id);
    if (it != rooms.end()) {
        return it->second; // Handle race condition if another thread created it
    }

    auto newRoom = std::make_shared<VoiceRoom>(room_id);
    rooms[room_id] = newRoom;
    return newRoom;
}

void ChatServerVoiceNode::broadcastAudioToRoom(const std::shared_ptr<VoiceRoom> &room, const std::string &sender_uid, const char *data, int length, int sender_socket)
{
    if (!room) return;

    // Use shared lock on the specific room so multiple broadcasters in different rooms can run concurrently
    std::shared_lock<std::shared_mutex> room_lock(room->room_mutex);
    for (const auto &pair : room->peers)
    {
        if (pair.first != sender_socket)
        {
            send(pair.first, data, length, MSG_NOSIGNAL);
        }
    }
}

void ChatServerVoiceNode::removePeerFromRoom(const std::string &room_id, int client_sock)
{
    std::shared_ptr<VoiceRoom> target_room;
    {
        std::shared_lock<std::shared_mutex> lock(global_rooms_mutex);
        auto it = rooms.find(room_id);
        if (it != rooms.end()) {
            target_room = it->second;
        }
    }

    if (target_room)
    {
        std::unique_lock<std::shared_mutex> room_lock(target_room->room_mutex);
        target_room->peers.erase(client_sock);
        std::cout << "[VOICE ROOM] Peer disconnected from room: " << room_id << std::endl;
    }
}

void ChatServerVoiceNode::run()
{
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "[VOICE NODE ERROR] Failed to create socket" << std::endl;
        return;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(voice_port);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        std::cerr << "[VOICE NODE ERROR] Failed to bind to port " << voice_port << std::endl;
        close(server_fd);
        return;
    }

    if (listen(server_fd, 128) < 0) { // Increased backlog for high concurrency
        std::cerr << "[VOICE NODE ERROR] Failed to listen on socket" << std::endl;
        close(server_fd);
        return;
    }

    std::cout << "[VOICE NODE] Concurrent multi-threaded voice streaming node listening on port " << voice_port << std::endl;

    while (running)
    {
        int client_sock = accept(server_fd, nullptr, nullptr);
        if (client_sock < 0)
            continue;

        // Spawn a dedicated thread for each concurrent client connection
        std::thread([this, client_sock]()
        {
            char buffer[4096] = {0};
            int valread = read(client_sock, buffer, sizeof(buffer) - 1);
            if (valread <= 0) {
                close(client_sock);
                return;
            }

            std::string payload(buffer, valread);

            auto extractField = [](const std::string& json, const std::string& key) {
                std::string sk = "\"" + key + "\":";
                size_t p = json.find(sk);
                if (p == std::string::npos) return std::string("");
                size_t s = p + sk.length();
                while (s < json.length() && (json[s] == ' ' || json[s] == '"')) s++;
                size_t e = s;
                while (e < json.length() && json[e] != '"' && json[e] != ',' && json[e] != '}') e++;
                return json.substr(s, e - s);
            };

            std::string action = extractField(payload, "action");
            std::string room_id = extractField(payload, "room_id");
            std::string uid = extractField(payload, "uid");

            if (action == "join_voice" && !room_id.empty() && !uid.empty()) {
                auto room = findOrCreateRoom(room_id);
                
                {
                    std::unique_lock<std::shared_mutex> room_lock(room->room_mutex);
                    room->peers[client_sock] = std::make_shared<VoicePeer>(uid, client_sock);
                    std::cout << "[VOICE ROOM] User " << uid << " joined room: " << room_id << " (Active Peers: " << room->peers.size() << ")" << std::endl;
                }

                std::string ok_resp = "{\"status\":\"voice_connected\"}";
                send(client_sock, ok_resp.c_str(), ok_resp.length(), 0);

                // High-performance concurrent audio stream loop
                char audio_buf[2048];
                while (running) {
                    int n = read(client_sock, audio_buf, sizeof(audio_buf));
                    if (n <= 0) break;
                    broadcastAudioToRoom(room, uid, audio_buf, n, client_sock);
                }

                // Cleanup on disconnect
                removePeerFromRoom(room_id, client_sock);
            }
            close(client_sock);
        }).detach();
    }
    close(server_fd);
}