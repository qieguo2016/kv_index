#include "kv_index/status.h"
#include "test_support/test_macros.h"

#include <memory>
#include <string>
#include <utility>

namespace {

void OkStatusHasOkCodeAndEmptyMessage() {
  const kv_index::Status status = kv_index::Status::Ok();

  KV_INDEX_CHECK(status.ok());
  KV_INDEX_CHECK_EQ(status.code(), kv_index::StatusCode::kOk);
  KV_INDEX_CHECK(status.message().empty());
}

void ErrorStatusCarriesCodeAndMessage() {
  const kv_index::Status status =
      kv_index::Status::InvalidArgument("shard count must be a power of two");

  KV_INDEX_CHECK(!status.ok());
  KV_INDEX_CHECK_EQ(status.code(), kv_index::StatusCode::kInvalidArgument);
  KV_INDEX_CHECK_EQ(status.message(),
                    "shard count must be a power of two");
}

void StatusOrHoldsValue() {
  kv_index::StatusOr<int> result(42);

  KV_INDEX_CHECK(result.ok());
  KV_INDEX_CHECK_EQ(result.status().code(), kv_index::StatusCode::kOk);
  KV_INDEX_CHECK_EQ(result.value(), 42);
  KV_INDEX_CHECK_EQ(*result, 42);
}

void StatusOrHoldsError() {
  kv_index::StatusOr<int> result(kv_index::Status::NotFound("missing row"));

  KV_INDEX_CHECK(!result.ok());
  KV_INDEX_CHECK_EQ(result.status().code(), kv_index::StatusCode::kNotFound);
  KV_INDEX_CHECK_EQ(result.status().message(), "missing row");
}

void StatusOrFailsClosedWhenConstructedFromOkStatusWithoutValue() {
  kv_index::StatusOr<int> result(kv_index::Status::Ok());

  KV_INDEX_CHECK(!result.ok());
  KV_INDEX_CHECK_EQ(result.status().code(), kv_index::StatusCode::kInternal);
  KV_INDEX_CHECK_NE(result.status().message().find("OK status"),
                    std::string::npos);
}

void StatusOrSupportsMoveOnlyValues() {
  kv_index::StatusOr<std::unique_ptr<int>> result(std::make_unique<int>(7));

  KV_INDEX_CHECK(result.ok());
  KV_INDEX_CHECK(*result != nullptr);
  KV_INDEX_CHECK_EQ(**result, 7);

  std::unique_ptr<int> moved = std::move(result).value();
  KV_INDEX_CHECK(moved != nullptr);
  KV_INDEX_CHECK_EQ(*moved, 7);
}

}  // namespace

int main() {
  OkStatusHasOkCodeAndEmptyMessage();
  ErrorStatusCarriesCodeAndMessage();
  StatusOrHoldsValue();
  StatusOrHoldsError();
  StatusOrFailsClosedWhenConstructedFromOkStatusWithoutValue();
  StatusOrSupportsMoveOnlyValues();
  return 0;
}
