#pragma once

#include <string>

#include "caf/all.hpp"
#include "caf/io/all.hpp"

using namespace caf;

using write_atom = atom_constant<caf::atom("write")>;

struct log_entry {
  std::string date;
  std::string host;
  u_int16_t status;
  u_int16_t size;
  std::string ip;
  std::string user_agend;
  std::string request;
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, log_entry& x) {
  return f(meta::type_name("log_entry"), x.date, x.host, x.status, x.size, x.ip,
           x.user_agend, x.request);
}
