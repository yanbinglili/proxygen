//
// Created by liyan on 2025-07-20.
//
// SamplingTransportFactory.h

#pragma once

#include <proxygen/httpserver/samples/hq/HQServer.h>
#include <proxygen/httpserver/samples/hq/HQParams.h>
#include <quic/server/QuicServerTransport.h>
#include <chrono>
#include <fstream>
#include <mutex>

namespace quic::samples {

class PerConnSampler : public std::enable_shared_from_this<PerConnSampler> {
 public:
  static std::shared_ptr<PerConnSampler> start(std::shared_ptr<QuicServerTransport> tr,
                    uint32_t intervalMs = 200);

  void stop();
 private:
  PerConnSampler(std::shared_ptr<QuicServerTransport> t, uint32_t ms)
      : transport_(std::move(t)), intervalMs_(ms) {}

  void schedule();

  void tick();

  std::shared_ptr<QuicServerTransport> transport_;
  uint32_t intervalMs_{200};
  std::atomic<bool> stopped_{false};

  uint32_t prevRetrans{0};
  uint32_t prevTotalPacketsSent{0};
  uint32_t prevTotalPacketsMarkedLost{0};
  uint32_t prevTotalPtoCount{0};

  uint64_t prevBytesSent{0};
  uint64_t prevBytesAcked{0};
  int64_t prevTickTimeMs_{0};
};


class SamplingTransportFactory : public HQServerTransportFactory{
 public:
  using Base = HQServerTransportFactory;
  SamplingTransportFactory(const HQServerParams& p,
                     HTTPTransactionHandlerProvider hp,
                     std::function<void(proxygen::HQSession*)> fn)
      : Base(p, std::move(hp), std::move(fn)) {}

  QuicServerTransport::Ptr make(
      folly::EventBase* evb,
      std::unique_ptr<quic::FollyAsyncUDPSocketAlias> sock,
      const folly::SocketAddress& peer,
      quic::QuicVersion ver,
      std::shared_ptr<const fizz::server::FizzServerContext> ctx) noexcept override;

  private:
    std::mutex mu_;
    std::weak_ptr<PerConnSampler> activeSampler_;
};
}
