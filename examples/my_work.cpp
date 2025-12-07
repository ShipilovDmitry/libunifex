#include <unifex/just.hpp>
#include <unifex/on.hpp>
#include <unifex/single_thread_context.hpp>
#include <unifex/sync_wait.hpp>
#include <unifex/task.hpp>
#include <unifex/then.hpp>
#include <unifex/when_all.hpp>
#include <chrono>
#include <future>
#include <iostream>
#include <memory>
#include <thread>

// 1. Legacy callback-based NetworkService interface
class NetworkService {
public:
  using Response = std::string;
  using Error = std::exception_ptr;

  void call(
      std::string request,
      std::function<void(Response)> on_success,
      std::function<void(Error)> on_error) {
    // Simulate async network call on "network thread"
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

// 2. Thread ID printer
void print_thread_info(const std::string& label) {
  auto id = std::this_thread::get_id();
  std::stringstream ss;
  ss << id;
  std::cout << "[" << label << "] Thread: " << ss.str().substr(0, 8) << "\n";
}

// 3. Coroutine wrapper for NetworkService
class NetworkServiceCoroutine {
  NetworkService& service_;
  unifex::single_thread_context& net_ctx_;

public:
  NetworkServiceCoroutine(
      NetworkService& service, unifex::single_thread_context& net_ctx)
    : service_(service)
    , net_ctx_(net_ctx) {}

  unifex::task<std::string> async_call(std::string request) {
    // Pin to network context
    co_await unifex::schedule(net_ctx_.get_scheduler());
    print_thread_info("NETWORK CALL: " + request);

    // Promise for async completion
    auto promise = std::make_shared<std::promise<std::string>>();

    // Call legacy callback API
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

    // Suspend until callback completes
    try {
      co_return co_await unifex::just(promise->get_future().get());
    } catch (...) {
      throw;  // Propagate network errors
    }
  }
};

// 4. CPU-intensive processing (runs on CPU context)
unifex::task<std::string>
process_data(unifex::single_thread_context& cpu_ctx, const std::string& data) {
  co_await unifex::schedule(cpu_ctx.get_scheduler());
  print_thread_info("CPU PROCESSING");

  // Simulate CPU work
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  std::string result = "PROCESSED_" + data;
  print_thread_info("CPU DONE: " + result.substr(0, 20));
  co_return result;
}

// 5. Full workflow with context switching
unifex::task<void> full_workflow(
    NetworkServiceCoroutine& net_coro, unifex::single_thread_context& cpu_ctx) {
  print_thread_info("WORKFLOW START");

  // Step 1: Network call #1 (NETWORK thread)
  std::cout << "\n=== STEP 1: Fetch users ===\n";
  auto users = co_await net_coro.async_call("GET /users");
  std::cout << "Users: " << users << "\n";

  // Step 2: CPU processing (CPU thread)
  std::cout << "\n=== STEP 2: Process data ===\n";
  auto processed = co_await process_data(cpu_ctx, users);

  // Step 3: Network call #2 with processed data (NETWORK thread)
  std::cout << "\n=== STEP 3: Submit processed ===\n";
  auto final_result = co_await net_coro.async_call("POST /submit/" + processed);
  std::cout << "Final: " << final_result << "\n";

  // Error case demo
  std::cout << "\n=== STEP 4: Error demo ===\n";
  try {
    co_await net_coro.async_call("GET /error-endpoint");
  } catch (const std::exception& e) {
    std::cout << "Handled error: " << e.what() << "\n";
  }

  print_thread_info("WORKFLOW COMPLETE");
}

int main() {
  std::cout << "=== NetworkService + Unifex Demo ===\n\n";

  unifex::single_thread_context net_ctx, cpu_ctx;
  NetworkService service;
  NetworkServiceCoroutine coro_service(service, net_ctx);

  unifex::sync_wait(full_workflow(coro_service, cpu_ctx));
  return 0;
}
