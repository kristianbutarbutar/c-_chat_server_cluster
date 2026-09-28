#include "chat_server_node.hpp"
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <thread>

ChatServerNode::ChatServerNode(int port, const std::string& db_conn) 
    : node_port(port), server_manager(db_conn) {}

void ChatServerNode::run() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(node_port);

    bind(server_fd, (struct sockaddr*)&address, sizeof(address));
    listen(server_fd, 20);

    std::cout << "[NODE PORT B] Chat Server Node listening on port " << node_port << std::endl;

    while (true) {
        int sock = accept(server_fd, nullptr, nullptr);
        if (sock < 0) continue;

        std::thread([this, sock]() {
            char buffer[8192] = {0};
            int valread = read(sock, buffer, 8192);
            if (valread > 0) {
                std::string request(buffer, valread);
                std::string response = server_manager.handleRequest(request);
                send(sock, response.c_str(), response.length(), 0);
            }
            close(sock);
        }).detach();
    }
    close(server_fd);
}