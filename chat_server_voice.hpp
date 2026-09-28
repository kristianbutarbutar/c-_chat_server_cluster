#ifndef CHAT_SERVER_VOICE_HPP
#define CHAT_SERVER_VOICE_HPP

#include <string>
#include <unordered_map>
#include <vector>
#include <shared_mutex>
#include <atomic>
#include <memory>

struct VoicePeer
{
    std::string uid;
    int socket_fd;

    VoicePeer(std::string u, int fd) : uid(std::move(u)), socket_fd(fd) {}
};

struct VoiceRoom
{
    std::string room_id;
    // Map of socket_fd -> VoicePeer for O(1) management and thread safety
    std::unordered_map<int, std::shared_ptr<VoicePeer>> peers;
    mutable std::shared_mutex room_mutex;

    explicit VoiceRoom(std::string r_id) : room_id(std::move(r_id)) {}
};

class ChatServerVoiceNode
{
private:
    int voice_port;
    std::atomic<bool> running;

    // Global room collection protected by its own shared mutex
    mutable std::shared_mutex global_rooms_mutex;
    std::unordered_map<std::string, std::shared_ptr<VoiceRoom>> rooms;

    std::shared_ptr<VoiceRoom> findOrCreateRoom(const std::string &room_id);
    void broadcastAudioToRoom(const std::shared_ptr<VoiceRoom> &room, const std::string &sender_uid, const char *data, int length, int sender_socket);
    void removePeerFromRoom(const std::string &room_id, int client_sock);

public:
    explicit ChatServerVoiceNode(int port);
    ~ChatServerVoiceNode();

    ChatServerVoiceNode(const ChatServerVoiceNode&) = delete;
    ChatServerVoiceNode& operator=(const ChatServerVoiceNode&) = delete;

    void run();
};

#endif // CHAT_SERVER_VOICE_HPP