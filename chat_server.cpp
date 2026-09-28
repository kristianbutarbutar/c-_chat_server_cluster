#include "chat_server.hpp"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <cctype>
#include <thread>
#include <pqxx/pqxx>

ChatServerManager::ChatServerManager(const std::string& db_conn) 
    : user_main_head(nullptr), member_main_head(nullptr), db_connection_string(db_conn) {}

ChatServerManager::~ChatServerManager() {
    std::unique_lock<std::shared_mutex> lock(rw_mutex);
    
    UserNode* curr_user = user_main_head;
    while (curr_user) {
        UserNode* next = curr_user->next;
        delete curr_user;
        curr_user = next;
    }

    MemberNode* curr_member = member_main_head;
    while (curr_member) {
        ChatMessageNode* curr_chat = curr_member->chat_head;
        while (curr_chat) {
            ChatMessageNode* next_chat = curr_chat->next;
            delete curr_chat;
            curr_chat = next_chat;
        }
        MemberNode* next_member = curr_member->next;
        delete curr_member;
        curr_member = next_member;
    }
}

std::string ChatServerManager::getCurrentTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d %H:%M:%S");
    return ss.str();
}

void ChatServerManager::logToDatabase(long long seqno, const std::string& uid, const std::string& touid_json, const std::string& message, const std::string& timestamp) {
    try {
        pqxx::connection conn(db_connection_string);
        if (conn.is_open()) {
            pqxx::work txn(conn);
            std::string query = "INSERT INTO chat_logs (seqno, uid, touid, message, timestamp, status) VALUES (" +
                                std::to_string(seqno) + ", " +
                                txn.quote(uid) + ", " +
                                txn.quote(touid_json) + ", " +
                                txn.quote(message) + ", " +
                                txn.quote(timestamp) + ", 'sent');";
            txn.exec(query);
            txn.commit();
        }
    } catch (const std::exception& e) {
        std::cerr << "[Database Log Error]: " << e.what() << std::endl;
    }
}

bool ChatServerManager::addUser(const std::string& uid, const std::string& name, int level, const std::string& status) {
    std::unique_lock<std::shared_mutex> lock(rw_mutex);
    UserNode* curr = user_main_head;
    while (curr) {
        if (curr->uid == uid) return false;
        curr = curr->next;
    }
    UserNode* newUser = new UserNode(uid, name, level, status);
    newUser->next = user_main_head;
    user_main_head = newUser;
    return true;
}

MemberNode* ChatServerManager::validate_node_members(const std::vector<std::string>& participants) {
    std::vector<std::string> sorted_participants = participants;
    std::sort(sorted_participants.begin(), sorted_participants.end());
    sorted_participants.erase(std::unique(sorted_participants.begin(), sorted_participants.end()), sorted_participants.end());

    MemberNode* curr = member_main_head;
    while (curr) {
        if (curr->members == sorted_participants) {
            std::cout << "[MEMBER VALIDATION] Existing MemberNode found. Reusing its Chat Linked List." << std::endl;
            return curr;
        }
        curr = curr->next;
    }

    std::cout << "[MEMBER VALIDATION] MemberNode not found. Creating new MemberNode and Chat Linked List." << std::endl;
    MemberNode* newMemberNode = new MemberNode(sorted_participants);
    newMemberNode->next = member_main_head;
    member_main_head = newMemberNode;
    return newMemberNode;
}

bool ChatServerManager::writeMessage(const std::string& uid, const std::vector<std::string>& touids, const std::string& message, const std::string& timestamp) {
    std::unique_lock<std::shared_mutex> lock(rw_mutex);

    std::vector<std::string> all_participants = touids;
    if (std::find(all_participants.begin(), all_participants.end(), uid) == all_participants.end()) {
        all_participants.push_back(uid);
    }

    MemberNode* room = validate_node_members(all_participants);
    if (!room) return false;

    long long assigned_seqno = ++room->sequence_counter;
    std::string ts = timestamp.empty() ? getCurrentTimestamp() : timestamp;
    
    std::string touid_str = "[";
    for (size_t i = 0; i < touids.size(); ++i) {
        touid_str += "\"" + touids[i] + "\"";
        if (i + 1 < touids.size()) touid_str += ",";
    }
    touid_str += "]";

    ChatMessageNode* newMsg = new ChatMessageNode(assigned_seqno, uid, touid_str, message, ts, "delivered");
    newMsg->next = room->chat_head;
    room->chat_head = newMsg;

    std::thread([=, this]() {
        logToDatabase(assigned_seqno, uid, touid_str, message, ts);
    }).detach();

    return true;
}

std::vector<ChatMessageNode> ChatServerManager::readMessages(const std::string& uid, const std::vector<std::string>& touids, long long fromseqno) {
    std::shared_lock<std::shared_mutex> lock(rw_mutex);
    std::vector<ChatMessageNode> result;

    std::vector<std::string> all_participants = touids;
    if (std::find(all_participants.begin(), all_participants.end(), uid) == all_participants.end()) {
        all_participants.push_back(uid);
    }

    std::vector<std::string> sorted_participants = all_participants;
    std::sort(sorted_participants.begin(), sorted_participants.end());
    sorted_participants.erase(std::unique(sorted_participants.begin(), sorted_participants.end()), sorted_participants.end());

    std::cout << "[READ_MESSAGE LOOKUP] Searching room with participants count: " << sorted_participants.size() 
              << " | fromseqno: " << fromseqno << std::endl;

    MemberNode* room = member_main_head;
    bool room_found = false;
    while (room) {
        if (room->members == sorted_participants) {
            room_found = true;
            ChatMessageNode* curr = room->chat_head;
            while (curr) {
                std::cout << "   -> Inspecting node seqno: " << curr->seqno 
                          << " | fromseqno: " << fromseqno 
                          << " | passes (>): " << (curr->seqno > fromseqno ? "YES (KEEP)" : "NO (SKIP)") << std::endl;

                if (curr->seqno > fromseqno) {
                    result.push_back(*curr);
                }
                curr = curr->next;
            }
            break;
        }
        room = room->next;
    }

    if (!room_found) {
        std::cout << "[READ_MESSAGE LOOKUP] WARNING: Matching MemberNode room NOT found!" << std::endl;
    }

    // Sort responses in ascending order
    std::reverse(result.begin(), result.end());

    return result;
}

static std::string extractJsonField(const std::string& json, const std::string& key) {
    std::string searchKey = "\"" + key + "\":";
    size_t pos = json.find(searchKey);
    if (pos == std::string::npos) {
        searchKey = key + ":";
        pos = json.find(searchKey);
        if (pos == std::string::npos) return "";
    }
    
    size_t start = pos + searchKey.length();
    while (start < json.length() && (json[start] == ' ' || json[start] == '\t' || json[start] == '\n' || json[start] == '\r')) {
        start++;
    }
    if (start >= json.length()) return "";

    if (json[start] == '"') {
        start++;
        size_t end = json.find("\"", start);
        if (end == std::string::npos) return "";
        return json.substr(start, end - start);
    } else {
        size_t end = start;
        while (end < json.length() && json[end] != ',' && json[end] != '}' && json[end] != ']' && json[end] != ' ' && json[end] != '\t' && json[end] != '\n' && json[end] != '\r') {
            end++;
        }
        return json.substr(start, end - start);
    }
}

static std::vector<std::string> extractJsonArray(const std::string& json, const std::string& key) {
    std::vector<std::string> list;
    std::string searchKey = "\"" + key + "\":";
    size_t pos = json.find(searchKey);
    if (pos == std::string::npos) return list;

    size_t bracketStart = json.find("[", pos);
    size_t bracketEnd = json.find("]", pos);
    if (bracketStart == std::string::npos || bracketEnd == std::string::npos) return list;

    std::string arrayContent = json.substr(bracketStart + 1, bracketEnd - bracketStart - 1);
    std::stringstream ss(arrayContent);
    std::string item;
    while (std::getline(ss, item, ',')) {
        size_t first = item.find_first_not_of(" \t\"");
        size_t last = item.find_last_not_of(" \t\"");
        if (first != std::string::npos && last != std::string::npos) {
            list.push_back(item.substr(first, (last - first + 1)));
        }
    }
    return list;
}

std::string ChatServerManager::handleRequest(const std::string& json_input) {
    if (json_input.find("\"action\":\"add_user\"") != std::string::npos) {
        std::string uid = extractJsonField(json_input, "uid");
        std::string name = extractJsonField(json_input, "name");
        std::string levelStr = extractJsonField(json_input, "level");
        std::string status = extractJsonField(json_input, "status");

        if (uid.empty()) return "{\"status\":\"error\",\"message\":\"Missing UID\"}";
        int level = levelStr.empty() ? 1 : std::stoi(levelStr);

        bool success = addUser(uid, name, level, status);
        return success ? "{\"status\":\"success\",\"message\":\"User added\"}" : "{\"status\":\"error\",\"message\":\"User exists\"}";
    }
    else if (json_input.find("\"action\":\"write_message\"") != std::string::npos) {
        std::string uid = extractJsonField(json_input, "uid");
        std::vector<std::string> touids = extractJsonArray(json_input, "touid");
        std::string message = extractJsonField(json_input, "message");
        std::string timestamp = extractJsonField(json_input, "timestamp");

        if (uid.empty() || touids.empty()) {
            return "{\"status\":\"error\",\"message\":\"Invalid payload fields\"}";
        }

        bool success = writeMessage(uid, touids, message, timestamp);
        return success ? "{\"status\":\"success\",\"message\":\"Message written\"}" : "{\"status\":\"error\",\"message\":\"Failed to write\"}";
    }
    else if (json_input.find("\"action\":\"read_message\"") != std::string::npos) {
        std::cout << "\n[SERVER REQUEST] read_message payload:\n" << json_input << std::endl;

        std::string uid = extractJsonField(json_input, "uid");
        std::vector<std::string> touids = extractJsonArray(json_input, "touid");
        std::string fromSeqStr = extractJsonField(json_input, "fromseqno");

        if (uid.empty()) return "{\"status\":\"error\",\"message\":\"Missing UID\"}";

        long long fromseqno = fromSeqStr.empty() ? 0 : std::stoll(fromSeqStr);
        std::vector<ChatMessageNode> messages = readMessages(uid, touids, fromseqno);

        std::ostringstream json_out;
        json_out << "{\"node\":[";
        for (size_t i = 0; i < messages.size(); ++i) {
            json_out << "{"
                     << "\"seqno\":" << messages[i].seqno << ","
                     << "\"uid\":\"" << messages[i].uid << "\","
                     << "\"touid\":" << messages[i].touid << ","
                     << "\"message\":\"" << messages[i].message << "\","
                     << "\"timestamp\":\"" << messages[i].timestamp << "\","
                     << "\"status\":\"" << messages[i].status << "\""
                     << "}";
            if (i + 1 < messages.size()) json_out << ",";
        }
        json_out << "]}";

        std::string response_payload = json_out.str();
        std::cout << "[SERVER RESPONSE] read_message output:\n" << response_payload << "\n" << std::endl;

        return response_payload;
    }

    return "{\"status\":\"error\",\"message\":\"Unknown action\"}";
}