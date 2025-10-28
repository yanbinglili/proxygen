//
// Created by liyan on 2025-07-20.
//
// SamplingTransportFactory.cpp
#include "SamplingTransportFactory.h"

#include "HQLoggerHelper.h"

#include <quic/congestion_control/AsyncLogger.h>
#include <folly/io/async/EventBase.h>
#include <proxygen/httpserver/samples/hq/AsyncSocketWriter.h>
#include <nlohmann/json.hpp>
using json = nlohmann::json;

using namespace quic::samples;

namespace quic {

extern thread_local folly::EventBase* current_evb_for_cc;
static const auto globalStartTime = std::chrono::steady_clock::now();


std::shared_ptr<PerConnSampler> PerConnSampler::start(std::shared_ptr<quic::QuicServerTransport> tr,
                  uint32_t intervalMs) {
  auto self = std::shared_ptr<PerConnSampler>(
      new PerConnSampler(std::move(tr), intervalMs));

  self->schedule();
  return self;
}

void PerConnSampler::stop() {
  auto evb = transport_->getEventBase();
  evb->runInEventBaseThread([self = shared_from_this()] {
    self->stopped_.store(true, std::memory_order_relaxed);
  });
}


void PerConnSampler::schedule() {
  if (stopped_.load(std::memory_order_relaxed)) return ;

  transport_->getEventBase()->runAfterDelay(
      [self = shared_from_this()] { self->tick(); }, intervalMs_);
}


void PerConnSampler::tick() {
  if (stopped_.load(std::memory_order_relaxed)) return ;

  quic::QuicSocketLite::TransportInfo info = transport_->getTransportInfo();
  auto cidOpt = transport_->getServerConnectionId();

  int steamCount = transport_->getState()->streamManager->streamCount();
  uint32_t deltaLost = info.totalPacketsMarkedLost - prevTotalPacketsMarkedLost;
  uint32_t deltaSent = info.totalPacketsSent - prevTotalPacketsSent;

  auto now = std::chrono::steady_clock::now();
  int64_t t_global_ms =
    std::chrono::duration_cast<std::chrono::milliseconds>(now - globalStartTime).count();

  int64_t real_interval = t_global_ms - prevTickTimeMs_;
  prevTickTimeMs_ = t_global_ms;

  double loss_rate = deltaSent > 0 ? static_cast<double>(deltaLost) / static_cast<double>(deltaSent) : 0.0;
  double send_rate = (info.bytesSent - prevBytesSent) * 8 / real_interval * 1000;
  double ack_rate = (info.bytesAcked - prevBytesAcked) * 8 / real_interval * 1000;

  using ojson = nlohmann::ordered_json;
  ojson j = {
    {"type", "quic"},
    {"hts", nowTimeString_ms()},
    {"ts", nowEpochMs()},
    {"server_conn_id", (cidOpt ? cidOpt->hex() : "no")},
    {"streams_count", steamCount},
    {"inflight", info.bytesInFlight},
    {"delta_retrans", info.packetsRetransmitted - prevRetrans},
    {"srtt_ms", std::chrono::duration_cast<std::chrono::milliseconds>(info.srtt).count()},
    {"rttvar_ms", std::chrono::duration_cast<std::chrono::milliseconds>(info.rttvar).count()},
    {"lrtt_ms", std::chrono::duration_cast<std::chrono::milliseconds>(info.lrtt).count()},
    {"send_rate_bps", send_rate},
    {"ack_rate_bps", ack_rate},
    {"loss_rate", loss_rate},
    {"pto_ms", std::chrono::duration_cast<std::chrono::milliseconds>(info.pto).count()},
    {"pto_count", info.ptoCount},
    {"delta_total_pto_count", info.totalPTOCount - prevTotalPtoCount},
    {"cctype", congestionControlTypeToString(info.congestionControlType)},
    {"cwnd", info.congestionWindow}
    };

  prevRetrans = info.packetsRetransmitted;
  prevTotalPacketsSent = info.totalPacketsSent;
  prevTotalPacketsMarkedLost = info.totalPacketsMarkedLost;
  prevBytesSent = info.bytesSent;
  prevBytesAcked = info.bytesAcked;
  prevTotalPtoCount = info.totalPTOCount;

  AsyncLogger::getInstance("quic").log(j.dump());
  AsyncSocketWriter::getInstance("cc_logs").write(j.dump());

  schedule();
}

quic::QuicServerTransport::Ptr SamplingTransportFactory::make(folly::EventBase* evb,
                         std::unique_ptr<quic::FollyAsyncUDPSocketAlias> socket,
                         const folly::SocketAddress& peer,
                         quic::QuicVersion ver,
                         std::shared_ptr<const fizz::server::FizzServerContext> ctx) noexcept {

  AsyncLogger::getInstance("quic").log("making the transport!");
  auto transport = HQServerTransportFactory::make(
      evb, std::move(socket), peer, ver, std::move(ctx));

  current_evb_for_cc = evb;

  {
    std::lock_guard<std::mutex> g(mu_);
    if (auto old = activeSampler_.lock()) {
      old->stop();
    }
    activeSampler_ = PerConnSampler::start(transport->shared_from_this());
  }

  return transport;
}
}
