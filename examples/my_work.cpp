#include <unifex/just.hpp>
#include <unifex/on.hpp>
#include <unifex/single_thread_context.hpp>
#include <unifex/sync_wait.hpp>
#include <unifex/task.hpp>
#include <iostream>
#include <thread>

void print_thread_info(const std::string& label) {
  auto id = std::this_thread::get_id();
  std::cout << "[" << label << "] Thread ID: " << id << "\n";
}

class ContextExecutor {
public:
  ContextExecutor(
      unifex::single_thread_context& cpu, unifex::single_thread_context& net)
    : cpu_(cpu)
    , net_(net) {}

  auto cpu_scheduler() const { return cpu_.get_scheduler(); }
  auto net_scheduler() const { return net_.get_scheduler(); }

private:
  unifex::single_thread_context& cpu_;
  unifex::single_thread_context& net_;
};

unifex::task<void> cpu_work() {
  print_thread_info("CPU WORK START");

  for (int i = 0; i < 3; ++i) {
    print_thread_info("CPU step " + std::to_string(i));
    co_await unifex::just();
  }
  print_thread_info("CPU WORK END");
}

unifex::task<void> network_work() {
  print_thread_info("NET WORK START");

  for (int i = 0; i < 2; ++i) {
    print_thread_info("NET step " + std::to_string(i));
    co_await unifex::just();
  }
  print_thread_info("NET WORK END");
}

unifex::task<void> demo(ContextExecutor& exec) {
  print_thread_info("MAIN START");

  std::cout << "\n=== NETWORK BLOCK ===\n";
  co_await unifex::schedule(exec.net_scheduler());
  co_await network_work();

  std::cout << "\n=== CPU BLOCK ===\n";
  co_await unifex::schedule(exec.cpu_scheduler());
  co_await cpu_work();

  std::cout << "\n=== BACK TO MAIN ===\n";
  print_thread_info("DEMO END");
}

int main() {
  unifex::single_thread_context cpu_ctx, net_ctx;
  ContextExecutor exec(cpu_ctx, net_ctx);

  unifex::sync_wait(demo(exec));
  return 0;
}
