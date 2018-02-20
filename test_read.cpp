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

plasma::PlasmaClient plasma_client_;

Status get_arrow_batch() {
  // Connect plasma client
  ARROW_RETURN_NOT_OK(
    plasma_client_.Connect("/tmp/plasma", "", PLASMA_DEFAULT_RELEASE_DELAY));
  // create and seal batch to plasma
  auto oid = plasma::ObjectID::from_binary("AAAAAAAAAAAAAAAAAAAA");
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
  auto status = get_arrow_batch();
  if (status.ok()) {
    std::cout << "batch is loaded from plasma" << std::endl;
  } else {
    std::cout << "error: " << status.message() << std::endl;
  }
}
