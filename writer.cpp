#include <string>

#include "arrow/api.h"
#include "arrow/builder.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/type.h"
#include "plasma/common.h"

#include "writer.hpp"

using namespace arrow;

std::vector<std::shared_ptr<Field>> schema_log_vector = {
  field("date", std::make_shared<StringType>()),
  field("host", std::make_shared<StringType>()),
  field("status", int16()),
  field("size", int16()),
  field("ip", std::make_shared<StringType>()),
  field("user_agent", std::make_shared<StringType>()),
  field("request", std::make_shared<StringType>()),
};

auto schema_log = std::make_shared<Schema>(schema_log_vector);

Writer::Writer(const std::string& p) : plasma_socket_(p) {
  auto status =
    plasma_client_.Connect(plasma_socket_, "", PLASMA_DEFAULT_RELEASE_DELAY);
  std::cout << status.message() << std::endl;
}

Writer::~Writer() {
  auto status = plasma_client_.Disconnect();
  if (!status.ok()) {
    std::cout << status.message() << std::endl;
  }
}

std::shared_ptr<RecordBatch> Writer::transpose(const log_entry& lev) {
  std::unique_ptr<RecordBatchBuilder> builder;
  std::shared_ptr<RecordBatch> batch;
  auto status =
    RecordBatchBuilder::Make(schema_log, default_memory_pool(), &builder);
  if (status.ok())
    status = builder->GetFieldAs<StringBuilder>(0)->Append(lev.date);
  if (status.ok())
    status = builder->GetFieldAs<StringBuilder>(1)->Append(lev.host);
  if (status.ok())
    status = builder->GetFieldAs<Int16Builder>(2)->Append(lev.status);
  if (status.ok())
    status = builder->GetFieldAs<Int16Builder>(3)->Append(lev.size);
  if (status.ok())
    status = builder->GetFieldAs<StringBuilder>(4)->Append(lev.ip);
  if (status.ok())
    status = builder->GetFieldAs<StringBuilder>(5)->Append(lev.user_agent);
  if (status.ok())
    status = builder->GetFieldAs<StringBuilder>(6)->Append(lev.request);
  if (status.ok())
    status = builder->Flush(&batch);
  if (!status.ok())
    std::cout << "Error: " << status.message() << std::endl;
  return batch;
}

std::shared_ptr<arrow::Buffer>
Writer::write_to_buffer(const arrow::RecordBatch& batch) {
  auto pool = arrow::default_memory_pool();
  auto buffer = std::make_shared<arrow::PoolBuffer>(pool);
  auto sink = std::make_unique<arrow::io::BufferOutputStream>(buffer);
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  auto status = arrow::ipc::RecordBatchStreamWriter::Open(sink.get(), batch.schema(), &writer);
  if (status.ok())
    status = writer->WriteRecordBatch(batch, true);
  if (status.ok())
    status = writer->Close();
  if (status.ok())
    status = sink->Close();
  if (!status.ok())
    std::cout << status.message() << std::endl;
  return buffer;
}

void Writer::write(std::shared_ptr<RecordBatch> batch) {
  auto buf = write_to_buffer(*batch);
  std::shared_ptr<Buffer> pBuf;
  auto oid = plasma::ObjectID::from_random();
  std::shared_ptr<arrow::Buffer> buffer;
  auto status = plasma_client_.Create(oid, buf->size(), nullptr,
                                      0, &pBuf);
  if (status.ok()) {
    for (uint32_t i = 0; i < buf->size();i++){
      std::cout << "i = " << i << std::endl;
    pBuf->mutable_data()[i] = buf->mutable_data()[i];
    }
    status = plasma_client_.Seal(oid);
  } else {
    std::cout << "Error: " << status.message() << std::endl;
  }
}

