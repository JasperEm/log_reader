#include <string>

#include "caf/all.hpp"
#include "caf/io/all.hpp"

#include "server.hpp"
#include "writer.hpp"

using namespace caf;

behavior server(std::shared_ptr<Writer> writer) {
  return {[=](write_atom, log_entry entry) {
    std::cout << "host: " << entry.host << ", date: " << entry.date
              << ", status: " << entry.status << ", size: " << entry.size
              << ", ip:" << entry.ip << ", user_agend: " << entry.user_agend
              << ", request: " << entry.request << std::endl;
    auto batch = writer->transpose(entry);
    writer->write(batch);
  }};
}

class config : public actor_system_config {
public:
  uint16_t port = 0;
  config() {
    add_message_type<log_entry>("log_entry");
    opt_group{custom_options_, "global"}.add(port, "port, p", "set port");
  }
};

void caf_main(actor_system& sys, const config& cfg) {
  //Writer writer("/tmp/plasma");
  auto writer = std::make_shared<Writer>("/tmp/plasma");
  auto a = sys.spawn(server, writer);
  auto state = sys.middleman().publish(a, cfg.port);

  if (state) {
    std::cout << "Server runs on port " << state << std::endl;
  } else {
    std::cout << "Server can`t bind given port`" << std::endl;
  }
}

CAF_MAIN(io::middleman)
