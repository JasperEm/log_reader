#include <string>

#include "arrow/api.h"
#include "arrow/builder.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "plasma/client.h"
#include "plasma/common.h"

using namespace arrow;

std::vector<std::shared_ptr<Field>> schema_log_vector_t = {
  field("date", std::make_shared<StringType>()),
  field("host", std::make_shared<StringType>()),
  field("status", int16()),
  field("size", int16()),
  field("ip", std::make_shared<StringType>()),
  field("user_agent", std::make_shared<StringType>()),
  field("request", std::make_shared<StringType>()),
};

auto schema_log_t = std::make_shared<Schema>(schema_log_vector_t);

plasma::PlasmaClient plasma_client_;

Status write_arrow_batch() {
  // Connect plasma client
  ARROW_RETURN_NOT_OK(
    plasma_client_.Connect("/tmp/plasma", "", PLASMA_DEFAULT_RELEASE_DELAY));
  // init builder
  std::unique_ptr<RecordBatchBuilder> builder;
  std::shared_ptr<RecordBatch> batch;
  ARROW_RETURN_NOT_OK(
    RecordBatchBuilder::Make(schema_log_t, default_memory_pool(), &builder));
  // add example data
  ARROW_RETURN_NOT_OK(builder->GetFieldAs<StringBuilder>(0)->Append(
    "20/Dec/2017:14:56:20 +0100"));
  ARROW_RETURN_NOT_OK(
    builder->GetFieldAs<StringBuilder>(1)->Append("localhost"));
  ARROW_RETURN_NOT_OK(builder->GetFieldAs<Int16Builder>(2)->Append(200));
  ARROW_RETURN_NOT_OK(builder->GetFieldAs<Int16Builder>(3)->Append(333));
  ARROW_RETURN_NOT_OK(
    builder->GetFieldAs<StringBuilder>(4)->Append("127.0.0.1"));
  ARROW_RETURN_NOT_OK(
    builder->GetFieldAs<StringBuilder>(5)->Append("Mozilla/5.0 (Linux)"));
  ARROW_RETURN_NOT_OK(builder->GetFieldAs<StringBuilder>(6)->Append("/"));
  ARROW_RETURN_NOT_OK(builder->Flush(&batch));
  // init batch writer
  auto pool = arrow::default_memory_pool();
  auto buf = std::make_shared<arrow::PoolBuffer>(pool);
  auto sink = std::make_unique<arrow::io::BufferOutputStream>(buf);
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  // write batch to buffer
  ARROW_RETURN_NOT_OK(arrow::ipc::RecordBatchStreamWriter::Open(
    sink.get(), batch->schema(), &writer));
  ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  ARROW_RETURN_NOT_OK(writer->Close());
  ARROW_RETURN_NOT_OK(sink->Close());
  // create and seal batch to plasma
  auto oid = plasma::ObjectID::from_binary("AAAAAAAAAAAAAAAAAAAA");
  std::shared_ptr<Buffer> buffer;
  std::cout << buf->size() << std::endl;
  ARROW_RETURN_NOT_OK(
    plasma_client_.Create(oid, buf->size(), nullptr, 0, &buffer));
  std::memcpy(buffer->mutable_data(), buf->data(), buf->size());
  ARROW_RETURN_NOT_OK(plasma_client_.Seal(oid));
  // load Batch from plasma
  plasma::ObjectBuffer pbuf;
  auto buf1 = plasma_client_.Get(&oid, 1, 10, &pbuf);
  std::shared_ptr<Buffer> ab;
  ab = pbuf.data;
  arrow::io::BufferReader bufferReader(ab);
  std::shared_ptr<arrow::RecordBatch> b;
  std::shared_ptr<arrow::ipc::RecordBatchReader> br;
  ARROW_RETURN_NOT_OK(
    arrow::ipc::RecordBatchStreamReader::Open(&bufferReader, &br));
  ARROW_RETURN_NOT_OK(br->ReadNext(&b));
  std::cout << b->schema()->ToString() << " " << b->num_rows() << std::endl;
  std::cout << b->column(0)->ToString() << " " << b->column(1)->ToString()
            << " " << b->column(2)->ToString() << " "
            << b->column(3)->ToString() << " " << b->column(4)->ToString()
            << " " << b->column(5)->ToString() << " "
            << b->column(6)->ToString() << std::endl;
  return Status::OK();
}

int main() {
  auto status = write_arrow_batch();
  if (status.ok()) {
    std::cout << "batch is written to plasma" << std::endl;
  } else {
    std::cout << "error: " << status.message() << std::endl;
  }
}
