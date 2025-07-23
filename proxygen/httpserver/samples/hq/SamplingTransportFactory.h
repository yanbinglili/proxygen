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
  static void start(std::shared_ptr<quic::QuicServerTransport> tr,
                    uint32_t intervalMs = 2000);
 private:
  PerConnSampler(std::shared_ptr<quic::QuicServerTransport> t, uint32_t ms)
      : transport_(std::move(t)), intervalMs_(ms) {}

  void schedule();

  void tick();

  std::shared_ptr<quic::QuicServerTransport> transport_;
  uint32_t intervalMs_;
};


class SamplingTransportFactory : public HQServerTransportFactory {
 public:
  using Base = HQServerTransportFactory;
  SamplingTransportFactory(const HQServerParams& p,
                     HTTPTransactionHandlerProvider hp,
                     std::function<void(proxygen::HQSession*)> fn)
      : Base(p, std::move(hp), std::move(fn)) {}

  quic::QuicServerTransport::Ptr make(
      folly::EventBase* evb,
      std::unique_ptr<quic::FollyAsyncUDPSocketAlias> sock,
      const folly::SocketAddress& peer,
      quic::QuicVersion ver,
      std::shared_ptr<const fizz::server::FizzServerContext> ctx) noexcept override;
};

}
