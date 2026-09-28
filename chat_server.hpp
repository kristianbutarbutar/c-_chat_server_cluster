#ifndef CHAT_SERVER_HPP
#define CHAT_SERVER_HPP

#include <string>
#include <vector>
#include <shared_mutex>
#include <chrono>

struct ChatMessageNode {
    long long seqno;
    std::string uid;
    std::string touid; // JSON array string
    std::string message;
    std::string timestamp;
    std::string status;
    ChatMessageNode* next;

    ChatMessageNode(long long s, std::string u, std::string to, std::string msg, std::string ts, std::string stat)
        : seqno(s), uid(u), touid(to), message(msg), timestamp(ts), status(stat), next(nullptr) {}
};

struct MemberNode {
    std::vector<std::string> members; // member string[] (UID + TOUIDs)
    ChatMessageNode* chat_head;       // points to Chat Linked List
    long long sequence_counter;       
    MemberNode* next;

    MemberNode(const std::vector<std::string>& m)
        : members(m), chat_head(nullptr), sequence_counter(0), next(nullptr) {}
};

struct UserNode {
    std::string uid;
    std::string name;
    int level;
    std::string status;
    UserNode* next;

    UserNode(std::string id, std::string n, int lvl, std::string stat)
        : uid(id), name(n), level(lvl), status(stat), next(nullptr) {}
};

class ChatServerManager {
private:
    UserNode* user_main_head;       // Main linked list for users
    MemberNode* member_main_head;   // Member linked list (by UID+TOUID)
    mutable std::shared_mutex rw_mutex;
    std::string db_connection_string;

    void logToDatabase(long long seqno, const std::string& uid, const std::string& touid_json, const std::string& message, const std::string& timestamp);
    std::string getCurrentTimestamp();
    MemberNode* validate_node_members(const std::vector<std::string>& participants);

public:
    ChatServerManager(const std::string& db_conn);
    ~ChatServerManager();

    bool addUser(const std::string& uid, const std::string& name, int level, const std::string& status);
    bool writeMessage(const std::string& uid, const std::vector<std::string>& touids, const std::string& message, const std::string& timestamp);
    std::vector<ChatMessageNode> readMessages(const std::string& uid, const std::vector<std::string>& touids, long long fromseqno);

    std::string handleRequest(const std::string& json_input);
};

#endif // CHAT_SERVER_HPP