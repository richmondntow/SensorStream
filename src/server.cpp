#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <poll.h>
#include <signal.h>
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cmath>
#include <condition_variable>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

static volatile sig_atomic_t stopping = 0;
static void stop_handler(int) { stopping = 1; }
struct Reading { double temperature; double battery; };
class Store {
    std::mutex mutex_; std::map<std::string, Reading> readings_;
public:
    void put(const std::string& id, Reading r) { std::lock_guard<std::mutex> lock(mutex_); readings_[id] = r; }
    std::string snapshot() {
        std::lock_guard<std::mutex> lock(mutex_); std::ostringstream out;
        out << "DATA " << readings_.size();
        for (const auto& [id, r] : readings_) out << ' ' << id << ',' << r.temperature << ',' << r.battery;
        return out.str() + "\n";
    }
};
class Queue {
    std::mutex mutex_; std::condition_variable cv_; std::queue<int> queue_; bool closed_ = false;
    static constexpr size_t capacity_ = 256;
public:
    bool push(int fd) { std::lock_guard<std::mutex> lock(mutex_); if (closed_ || queue_.size() >= capacity_) return false; queue_.push(fd); cv_.notify_one(); return true; }
    bool pop(int& fd) { std::unique_lock<std::mutex> lock(mutex_); cv_.wait(lock, [&]{ return closed_ || !queue_.empty(); }); if (queue_.empty()) return false; fd = queue_.front(); queue_.pop(); return true; }
    void close() { std::lock_guard<std::mutex> lock(mutex_); closed_ = true; while (!queue_.empty()) { ::close(queue_.front()); queue_.pop(); } cv_.notify_all(); }
};
static bool send_all(int fd, const std::string& s) {
    size_t offset = 0; while (offset < s.size()) { ssize_t n = send(fd, s.data() + offset, s.size() - offset, 0); if (n < 0 && errno == EINTR) continue; if (n <= 0) return false; offset += static_cast<size_t>(n); } return true;
}
static std::string process(const std::string& line, Store& store) {
    std::istringstream in(line); std::string command, id, extra; in >> command;
    if (command == "GET" && !(in >> extra)) return store.snapshot();
    double temp, battery;
    if (command == "PUT" && (in >> id >> temp >> battery) && !(in >> extra) && !id.empty() && id.size() <= 64 && id.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") == std::string::npos && std::isfinite(temp) && std::isfinite(battery) && temp >= -100 && temp <= 200 && battery >= 0 && battery <= 100) { store.put(id, {temp, battery}); return "OK\n"; }
    return "ERR invalid request\n";
}
static void handle(int fd, Store& store) {
    // A short socket timeout allows shutdown and limits worker exhaustion by idle clients.
    timeval timeout{1, 0}; setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)); setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    std::string pending; char buf[1024];
    while (!stopping) {
        ssize_t n = recv(fd, buf, sizeof(buf), 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) break;
        pending.append(buf, static_cast<size_t>(n));
        size_t pos;
        while ((pos = pending.find('\n')) != std::string::npos) {
            if (pos > 512) { send_all(fd, "ERR request too long\n"); return; }
            auto line = pending.substr(0, pos); pending.erase(0, pos + 1);
            if (!send_all(fd, process(line, store))) return;
        }
        if (pending.size() > 512) { send_all(fd, "ERR request too long\n"); break; }
    }
}
int main(int argc, char** argv) {
    int port = 9000, workers = 4;
    try { if (argc > 3) throw 1; if (argc > 1) port = std::stoi(argv[1]); if (argc > 2) workers = std::stoi(argv[2]); if (port < 1 || port > 65535 || workers < 1 || workers > 128) throw 1; } catch (...) { std::cerr << "Usage: sensorstream_server [port 1-65535] [workers 1-128]\n"; return 1; }
    signal(SIGINT, stop_handler); signal(SIGTERM, stop_handler); signal(SIGPIPE, SIG_IGN);
    int listener = socket(AF_INET, SOCK_STREAM, 0); if (listener < 0) { perror("socket"); return 1; }
    int reuse = 1; setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(static_cast<uint16_t>(port)); addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0 || listen(listener, 128) < 0) { perror("bind/listen"); close(listener); return 1; }
    Store store; Queue queue; std::vector<std::thread> pool;
    for (int i = 0; i < workers; ++i) pool.emplace_back([&]{ int fd; while (queue.pop(fd)) { handle(fd, store); close(fd); } });
    std::cout << "Listening on 127.0.0.1:" << port << " with " << workers << " workers" << std::endl;
    while (!stopping) { pollfd p{listener, POLLIN, 0}; int ready = poll(&p, 1, 200); if (ready < 0 && errno != EINTR) { perror("poll"); break; } if (ready > 0 && (p.revents & POLLIN)) { int fd = accept(listener, nullptr, nullptr); if (fd >= 0 && !queue.push(fd)) close(fd); } }
    close(listener); queue.close(); for (auto& thread : pool) thread.join(); return 0;
}
