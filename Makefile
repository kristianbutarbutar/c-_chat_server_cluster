CXX = g++
CXXFLAGS = -std=c++20 -pthread -Wall -I/usr/local/include -I/usr/local/opt/libpq/include -I/usr/local/opt/libpqxx/include
LDFLAGS = -L/usr/local/lib -L/usr/local/opt/libpq/lib -L/usr/local/opt/libpqxx/lib -lpqxx -lpq

TARGET = chat_server_cluster
SRCS = main.cpp chat_server.cpp chat_server_node.cpp chat_server_master.cpp chat_server_voice.cpp

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CXX) $(CXXFLAGS) $(SRCS) -o $(TARGET) $(LDFLAGS)

clean:
	rm -f $(TARGET)