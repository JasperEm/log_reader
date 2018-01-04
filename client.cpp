#include <iostream>
#include <regex>
#include <string>

#include <chrono>
#include <thread>

#include "server.hpp"

using namespace caf;

using parse_atom = atom_constant<atom("parse")>;

behavior parser(event_based_actor* self, actor server) {
  return {[=](parse_atom, std::string file) {
    std::ifstream f(file);
    bool braces_open = false;
    bool quotes_open = false;
    auto words = std::vector<std::string>();
    log_entry entry;

    std::string line;
    std::string word;
    while (true) {
      char c = f.peek();
      if (EOF == c) {
        f.clear();
        std::this_thread::sleep_for(std::chrono::seconds(5));
        continue;
      }
      getline(f, line);
      for (u_int16_t i = 0; i < line.size(); i++) {
        if ('[' == line[i] && !quotes_open) {
          braces_open = true;
          continue;
        } else if (']' == line[i] && !quotes_open) {
          braces_open = false;
          words.push_back(word);
          word.clear();
          continue;
        } else if ('"' == line[i] && !braces_open) {
          if (quotes_open) {
            words.push_back(word);
            word.clear();
          }
          quotes_open = !quotes_open;
          continue;
        } else if (' ' == line[i] && !(braces_open || quotes_open)) {
          words.push_back(word);
          word.clear();
          continue;
        }
        word += line[i];
      }
      entry.host = words[0];
      if (15 == words.size()) {
        entry.date = words[4];
        entry.ip = words[1];
        entry.size = std::stoi(words[11]);
        entry.status = std::stoi(words[10]);
        entry.user_agend = words[14];
        entry.request = words[8];
      } else {
        entry.date = words[3];
        entry.status = std::stoi(words[7]);
        entry.size = std::stoi(words[8]);
        entry.user_agend = words[11];
        entry.request = words[5];
      }
      self->send(server, write_atom::value, entry);
      words.clear();
    }
  }};
}

class config : public actor_system_config {
public:
  u_int16_t port = 0;
  std::string hostname = "localhost";
  std::string file;

  config() {
    add_message_type<log_entry>("log_entry");
    opt_group{custom_options_, "global"}
      .add(port, "port, p", "set port")
      .add(hostname, "hostname, h", "set hostname")
      .add(file, "file, f", "log file");
  }
};

void caf_main(actor_system& sys, const config& cfg) {

  auto server = sys.middleman().remote_actor(cfg.hostname, cfg.port);
  std::cout << server.error().code() << std::endl;
  if (server.error().code() > 4) {
    std::cout << "can`t  connect to server" << std::endl;
    exit(1);
  }
  std::cout << "Connected to " << cfg.hostname << ":" << cfg.port << "\n"
            << std::endl;

  scoped_actor self{sys};
  auto a = sys.spawn(parser, *server);

  self->send(a, parse_atom::value, cfg.file);
}
CAF_MAIN(io::middleman)

