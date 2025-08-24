//
// Created by liyan on 2025-07-26.
//

#pragma once

#include <folly/ProducerConsumerQueue.h>
#include <string>
#include <thread>
#include <atomic>

std::string nowTimeString();
std::string nowTimeString_ms();
inline long long nowEpochMs() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

class AsyncSocketWriter {
public:

  static AsyncSocketWriter& getInstance(const std::string& name);

  AsyncSocketWriter(const AsyncSocketWriter&) = delete;
  void operator=(const AsyncSocketWriter&) = delete;

  void write(std::string&& message);

  static void stopAll();
  void stop();

  ~AsyncSocketWriter();

private:
  explicit AsyncSocketWriter(const std::string& socketPath);
  void threadLoop();
  void connect();

  int sock_fd_ = -1;
  std::string socket_path_;
  folly::ProducerConsumerQueue<std::string> queue_;

  std::atomic<bool> done_;
  std::thread writerThread_;
};