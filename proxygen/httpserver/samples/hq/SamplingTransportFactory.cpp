//
// Created by liyan on 2025-07-20.
//
// SamplingTransportFactory.cpp
#include "SamplingTransportFactory.h"

using namespace quic::samples;

namespace{
    std::string nowTimeString() {
        using namespace std::chrono;
        auto tp = system_clock::now();
        std::time_t t = system_clock::to_time_t(tp);
        char buf[32];
        std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
        return std::string(buf);
    }
}


void PerConnSampler::start(std::shared_ptr<quic::QuicServerTransport> tr,
                  uint32_t intervalMs) {
    auto self = std::shared_ptr<PerConnSampler>(
        new PerConnSampler(std::move(tr), intervalMs));
    self->schedule();
}


void PerConnSampler::schedule() {
    transport_->getEventBase()->runAfterDelay(
        [self = shared_from_this()] { self->tick(); }, intervalMs_);
}


void PerConnSampler::tick() {
    quic::QuicSocketLite::TransportInfo info = transport_->getTransportInfo();

    static std::mutex mu;
    static std::ofstream ofs("/home/liyan/proxygen/log/quic.log",
                             std::ios::app);

    auto cidOpt = transport_->getClientConnectionId();

    {
        std::lock_guard<std::mutex> g(mu);
        ofs << "ts="   << nowTimeString()
            << " cid="    << (cidOpt ? cidOpt->hex() : "no")
            << " inflight="  << info.bytesInFlight
            << " retrans="  << info.packetsRetransmitted
            << " srtt_ms="
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   info.srtt)
                   .count()
            << " cwnd="  << info.congestionWindow
            << "\n";
    }

    schedule();
}

quic::QuicServerTransport::Ptr SamplingTransportFactory::make(folly::EventBase* evb,
                         std::unique_ptr<quic::FollyAsyncUDPSocketAlias> socket,
                         const folly::SocketAddress& peer,
                         quic::QuicVersion ver,
                         std::shared_ptr<const fizz::server::FizzServerContext> ctx) noexcept {

  auto transport = HQServerTransportFactory::make(
      evb, std::move(socket), peer, ver, std::move(ctx));

  PerConnSampler::start(transport->shared_from_this(), 2000);

  return transport;
}
