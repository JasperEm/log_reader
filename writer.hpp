#pragma once

#include <string>

#include "arrow/api.h"
#include "arrow/builder.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/type.h"
#include "plasma/client.h"
#include "plasma/common.h"

#include "server.hpp"

class Writer {
private:
  const std::string& plasma_socket_;
  plasma::PlasmaClient plasma_client_;
  std::shared_ptr<arrow::Buffer>
  write_to_buffer(const arrow::RecordBatch& batch);

public:
  Writer(const std::string& plasma_socket);
  ~Writer();
  std::shared_ptr<arrow::RecordBatch> transpose(const log_entry& lev);
  void write(std::shared_ptr<arrow::RecordBatch> batch);
};
