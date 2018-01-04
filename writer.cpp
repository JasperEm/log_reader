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
  field("user_agend", std::make_shared<StringType>()),
  field("request", std::make_shared<StringType>()),
};

auto schema_log = std::make_shared<Schema>(schema_log_vector);

Writer::Writer(const std::string& p) : plasma_socket_(p) {
  auto status
    = plasma_client_.Connect(plasma_socket_, "", PLASMA_DEFAULT_RELEASE_DELAY);
  std::cout << status.message() << std::endl;

}

Writer::~Writer() {
  plasma_client_.Disconnect();
}

std::shared_ptr<RecordBatch> Writer::transpose(const log_entry& lev) {
  std::unique_ptr<RecordBatchBuilder> builder;
  std::shared_ptr<RecordBatch> batch;
  RecordBatchBuilder::Make(schema_log, default_memory_pool(), &builder);

  builder->GetFieldAs<StringBuilder>(0)->Append(lev.date);
  builder->GetFieldAs<StringBuilder>(1)->Append(lev.host);
  builder->GetFieldAs<Int16Builder>(2)->Append(lev.status);
  builder->GetFieldAs<Int16Builder>(3)->Append(lev.size);
  builder->GetFieldAs<StringBuilder>(4)->Append(lev.ip);
  builder->GetFieldAs<StringBuilder>(5)->Append(lev.user_agend);
  builder->GetFieldAs<StringBuilder>(6)->Append(lev.request);
  builder->Flush(&batch);
  return batch;
}

std::shared_ptr<arrow::Buffer>
Writer::write_to_buffer(const arrow::RecordBatch& batch) {
  auto pool = arrow::default_memory_pool();
  auto buffer = std::make_shared<arrow::PoolBuffer>(pool);
  auto sink = std::make_unique<arrow::io::BufferOutputStream>(buffer);
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  auto status = arrow::ipc::RecordBatchStreamWriter::Open(
    sink.get(), batch.schema(), &writer);
  if (status.ok())
    status = writer->WriteRecordBatch(batch);
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
  auto oid = plasma::ObjectID::from_random();
  uint8_t* buffer;
  auto status =
    plasma_client_.Create(oid, batch->num_rows(), nullptr, 0, &buffer);
  if (status.ok()) {
    std::memcpy(buffer, reinterpret_cast<const char*>((*buf).data()),
                static_cast<size_t>((*buf).size()));
    status = plasma_client_.Seal(oid); 
  }
}

