#include <string>

#include "caf/all.hpp"
#include "caf/io/all.hpp"

#include "arrow/api.h"
#include "arrow/builder.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/type.h"
#include "plasma/common.h"

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

  std::vector<std::shared_ptr<arrow::Field>> schema_log = {
    arrow::field("date", std::make_shared<arrow::StringType>()),
    arrow::field("host", std::make_shared<arrow::StringType>()),
    arrow::field("status", arrow::int16()),
    arrow::field("size", arrow::int16()),
    arrow::field("ip", std::make_shared<arrow::StringType>()),
    arrow::field("user_agend", std::make_shared<arrow::StringType>()),
    arrow::field("request", std::make_shared<arrow::StringType>()),
  };


template <class Inspector>
typename Inspector::result_type inspect(Inspector &f, log_entry &x) {
  return f(meta::type_name("log_entry"), x.date, x.host, x.status, x.size,
           x.ip, x.user_agend, x.request);
}


