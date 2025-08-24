//
// Created by liyan on 2025-07-26.
//
// common/AsyncSocketWriter.cpp

#include "AsyncSocketWriter.h"
#include <iostream>
#include <map>
#include <mutex>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

static std::map<std::string, std::unique_ptr<AsyncSocketWriter>> writers;
static std::mutex writersMutex;

std::string nowTimeString() {
  using namespace std::chrono;
  auto tp = system_clock::now();
  std::time_t t = system_clock::to_time_t(tp);
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
  return std::string(buf);
}

std::string nowTimeString_ms() {
  using namespace std::chrono;

  const auto now = system_clock::now();
  const auto tt  = system_clock::to_time_t(now);

  std::tm tm{};
  localtime_r(&tt, &tm);

  char hms[16];
  std::strftime(hms, sizeof(hms), "%H:%M:%S", &tm);

  const auto ms_since_epoch = duration_cast<milliseconds>(now.time_since_epoch()).count();
  const long ms = static_cast<long>(ms_since_epoch % 1000);

  char out[24];
  std::snprintf(out, sizeof(out), "%s.%03ld", hms, ms);
  return out;
}

AsyncSocketWriter& AsyncSocketWriter::getInstance(const std::string& name) {
  std::lock_guard<std::mutex> guard(writersMutex);

  auto it = writers.find(name);
  if (it == writers.end()) {
    std::string socketPath = "/tmp/" + name + ".sock";
    it = writers.emplace(name, std::unique_ptr<AsyncSocketWriter>(new AsyncSocketWriter(socketPath))).first;
  }
  return *it->second;
}

void AsyncSocketWriter::stopAll() {
  std::lock_guard<std::mutex> guard(writersMutex);
  for (auto& pair : writers) {
    if (pair.second) {
      pair.second->stop();
    }
  }
  writers.clear();
}

AsyncSocketWriter::AsyncSocketWriter(const std::string& socketPath) :
    socket_path_(socketPath),
    queue_(8192),
    done_(false) {
  connect();
  writerThread_ = std::thread(&AsyncSocketWriter::threadLoop, this);
}

void AsyncSocketWriter::connect() {
    if ((sock_fd_ = socket(AF_UNIX, SOCK_DGRAM, 0)) < 0) {
        perror("socket error");
        return;
    }

    struct sockaddr_un remote;
    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, socket_path_.c_str(), sizeof(remote.sun_path) - 1);

    if (::connect(sock_fd_, (struct sockaddr *)&remote, sizeof(remote)) < 0) {
        perror("connect error");
        close(sock_fd_);
        sock_fd_ = -1;
    }
}

void AsyncSocketWriter::write(std::string&& message) {
    if (done_ || sock_fd_ < 0) return;
    while (!queue_.write(std::move(message))) {}
}

void AsyncSocketWriter::stop() {
    if (done_.exchange(true)) return;
    if (writerThread_.joinable()) {
        writerThread_.join();
    }
    if (sock_fd_ >= 0) {
        close(sock_fd_);
    }
}

AsyncSocketWriter::~AsyncSocketWriter() {
  stop();
}

void AsyncSocketWriter::threadLoop() {
    std::string msg;
    while (!done_) {
        if (queue_.read(msg)) {
            if (sock_fd_ < 0) continue;

            ssize_t sent_bytes = send(sock_fd_, msg.c_str(), msg.length(), 0);
            if (sent_bytes < 0) {
                perror("send error");
                // 简单的重连策略
                close(sock_fd_);
                sock_fd_ = -1;
                connect();
            }
        }
    }

    while (queue_.read(msg)) {
       if (sock_fd_ >= 0) {
           send(sock_fd_, msg.c_str(), msg.length(), 0);
       }
    }
}