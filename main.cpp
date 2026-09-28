#include <iostream>
#include <string>
#include <cstdlib>
#include <fstream>
#include "chat_server_master.hpp"
#include "chat_server_node.hpp"
#include "chat_server_voice.hpp"

void printUsage(const char *progName)
{
    std::cout << "Usage:\n"
              << "  To run as Master Controller: " << progName << " master <config_file>\n"
              << "  To run as Worker Node:       " << progName << " node <port> <config_file>\n"
              << "  To run as Voice Node:        " << progName << " voice <port>\n";
}

// Helper to parse db_conn string from config file if needed
std::string parseDbConnFromConfig(const std::string &configFile)
{
    std::ifstream file(configFile);
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
        if (key == "db_conn")
        {
            size_t first = val.find_first_not_of(" \t\r\n");
            size_t last = val.find_last_not_of(" \t\r\n");
            if (first != std::string::npos && last != std::string::npos)
            {
                return val.substr(first, (last - first + 1));
            }
        }
    }
    return "dbname=amgreatdb user=amgreat password=amgreat host=localhost port=5432";
}

int main(int argc, char *argv[])
{
    if (argc < 3)
    {
        printUsage(argv[0]);
        return 1;
    }

    std::string role = argv[1];

    if (role == "master")
    {
        std::string configFile = argv[2];
        std::cout << "[MAIN] Starting Chat Server Master Controller using config: " << configFile << std::endl;

        ChatServerMaster master(configFile);
        master.run();
    }
    else if (role == "node")
    {
        if (argc < 4)
        {
            printUsage(argv[0]);
            return 1;
        }
        int port = std::stoi(argv[2]);
        std::string configFile = argv[3];

        std::string db_conn = parseDbConnFromConfig(configFile);

        std::cout << "[MAIN] Starting Chat Server Worker Node on port " << port << std::endl;
        ChatServerNode workerNode(port, db_conn);
        workerNode.run();
    }
    else if (role == "voice")
    {
        int port = std::stoi(argv[2]);
        std::cout << "[MAIN] Starting Chat Server Voice Streaming Node on port " << port << std::endl;

        ChatServerVoiceNode voiceNode(port);
        voiceNode.run();
    }
    else
    {
        std::cerr << "[MAIN ERROR] Unknown role specified: " << role << std::endl;
        printUsage(argv[0]);
        return 1;
    }

    return 0;
}