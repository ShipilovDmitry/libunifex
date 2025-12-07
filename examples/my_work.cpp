#include <unifex/just.hpp>
#include <unifex/on.hpp>
#include <unifex/single_thread_context.hpp>
#include <unifex/sync_wait.hpp>
#include <unifex/task.hpp>
#include <chrono>
#include <future>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <thread>

// 1. Legacy callback-based NetworkService interface (UNCHANGED)
class NetworkService {
public:
  using Response = std::string;
  using Error = std::exception_ptr;

  void call(
      std::string request,
      std::function<void(Response)> on_success,
      std::function<void(Error)> on_error) {
    std::thread([request = std::move(request),
                 on_success = std::move(on_success),
                 on_error = std::move(on_error)]() mutable {
      std::this_thread::sleep_for(
          std::chrono::milliseconds(100 + rand() % 400));

      if (request.find("error") != std::string::npos) {
        on_error(
            std::make_exception_ptr(std::runtime_error("Network failure")));
      } else {
        on_success("Response for: " + request);
      }
    }).detach();
  }
};

// 2. Thread ID printer (ENHANCED)
void print_thread_info(const std::string& label) {
  auto id = std::this_thread::get_id();
  std::stringstream ss;
  ss << id;
  std::cout << "[ " << std::setw(20) << label
            << " ] Thread: " << ss.str().substr(0, 8) << "\n";
}

// 3. Coroutine wrapper (UNCHANGED - runs on NETWORK thread)
class NetworkServiceCoroutine {
  NetworkService& service_;
  unifex::single_thread_context& net_ctx_;

public:
  NetworkServiceCoroutine(
      NetworkService& service, unifex::single_thread_context& net_ctx)
    : service_(service)
    , net_ctx_(net_ctx) {}

  unifex::task<std::string> async_call(std::string request) {
    // ✅ EXPLICIT: Pin to NETWORK thread
    co_await unifex::schedule(net_ctx_.get_scheduler());
    print_thread_info("🌐 NETWORK CALL: " + request);

    auto promise = std::make_shared<std::promise<std::string>>();

    service_.call(
        std::move(request),
        [promise](std::string response) {
          promise->set_value(std::move(response));
        },
        [promise](std::exception_ptr err) {
          try {
            std::rethrow_exception(err);
          } catch (const std::exception& e) {
            promise->set_exception(std::make_exception_ptr(e));
          }
        });

    try {
      co_return co_await unifex::just(promise->get_future().get());
    } catch (...) {
      throw;
    }
  }
};

// 4. CPU processing (EXPLICITLY pinned to CPU thread)
unifex::task<std::string>
process_data(unifex::single_thread_context& cpu_ctx, const std::string& data) {
  // ✅ EXPLICIT: Pin to CPU thread
  co_await unifex::schedule(cpu_ctx.get_scheduler());
  print_thread_info("🧠 CPU PROCESSING");

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  std::string result = "PROCESSED_" + data;
  print_thread_info("✅ CPU DONE: " + result.substr(0, 20));
  co_return result;
}

// 5. DEMO: Network → CPU workflow
unifex::task<void> network_to_cpu_demo(
    NetworkServiceCoroutine& net_coro, unifex::single_thread_context& cpu_ctx) {
  print_thread_info("🚀 MAIN WORKFLOW START");

  std::cout << "\n" << std::string(60, '=') << "\n";
  std::cout << "🌐 NETWORK CALL → 🧠 CPU PROCESS\n";
  std::cout << std::string(60, '=') << "\n\n";

  // STEP 1: Network call (NETWORK thread)
  print_thread_info("📡 Step 1: Starting network request");
  std::string raw_data;
  try {
    raw_data = co_await net_coro.async_call("GET /api/raw-data");
  } catch (std::exception const& e) {
    // on failure
    co_return;
  }
  std::cout << "📥 Raw response: " << raw_data << "\n\n";

  // STEP 2: EXPLICIT switch to CPU thread for processing
  print_thread_info("🔄 Step 2: Switching to CPU thread...");
  auto processed_data = co_await process_data(cpu_ctx, raw_data);
  std::cout << "🔬 Processed: " << processed_data << "\n\n";

  print_thread_info("🎉 WORKFLOW COMPLETE");
}

int main() {
  std::cout << "=== Network → CPU Thread Switching Demo ===\n\n";

  // Thread contexts
  unifex::single_thread_context net_ctx, cpu_ctx;
  NetworkService service;
  NetworkServiceCoroutine coro_service(service, net_ctx);

  // Run demo
  unifex::sync_wait(network_to_cpu_demo(coro_service, cpu_ctx));

  std::cout << "\n✅ Demo complete - saw thread switching!\n";
  return 0;
}
