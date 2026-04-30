#ifndef KV_INDEX_STATUS_H_
#define KV_INDEX_STATUS_H_

#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

namespace kv_index {

enum class StatusCode {
  kOk = 0,
  kCancelled,
  kInvalidArgument,
  kNotFound,
  kFailedPrecondition,
  kInternal,
  kUnavailable,
  kUnknown,
};

class Status {
 public:
  Status() = default;

  Status(StatusCode code, std::string message)
      : code_(code), message_(std::move(message)) {
    if (code_ == StatusCode::kOk && !message_.empty()) {
      code_ = StatusCode::kUnknown;
    }
  }

  static Status Ok() { return Status(); }

  static Status Error(StatusCode code, std::string message) {
    return Status(code, std::move(message));
  }

  static Status Cancelled(std::string message) {
    return Error(StatusCode::kCancelled, std::move(message));
  }

  static Status InvalidArgument(std::string message) {
    return Error(StatusCode::kInvalidArgument, std::move(message));
  }

  static Status NotFound(std::string message) {
    return Error(StatusCode::kNotFound, std::move(message));
  }

  static Status FailedPrecondition(std::string message) {
    return Error(StatusCode::kFailedPrecondition, std::move(message));
  }

  static Status Internal(std::string message) {
    return Error(StatusCode::kInternal, std::move(message));
  }

  static Status Unavailable(std::string message) {
    return Error(StatusCode::kUnavailable, std::move(message));
  }

  static Status Unknown(std::string message) {
    return Error(StatusCode::kUnknown, std::move(message));
  }

  bool ok() const noexcept { return code_ == StatusCode::kOk; }
  StatusCode code() const noexcept { return code_; }
  const std::string& message() const noexcept { return message_; }
  explicit operator bool() const noexcept { return ok(); }

 private:
  StatusCode code_ = StatusCode::kOk;
  std::string message_;
};

template <typename T>
class StatusOr {
 public:
  StatusOr(const Status& status)
      : storage_(std::in_place_index<0>, NormalizeStatus(status)) {}

  StatusOr(Status&& status)
      : storage_(std::in_place_index<0>, NormalizeStatus(std::move(status))) {}

  template <
      typename U = T,
      typename = std::enable_if_t<
          std::is_constructible_v<T, U&&> &&
          !std::is_same_v<std::remove_cvref_t<U>, StatusOr> &&
          !std::is_same_v<std::remove_cvref_t<U>, Status>>>
  StatusOr(U&& value)
      : storage_(std::in_place_index<1>, std::forward<U>(value)) {}

  bool ok() const noexcept { return storage_.index() == 1; }
  explicit operator bool() const noexcept { return ok(); }

  const Status& status() const noexcept {
    if (ok()) {
      return OkStatus();
    }
    return std::get<0>(storage_);
  }

  T& value() & {
    EnsureOk();
    return std::get<1>(storage_);
  }

  const T& value() const& {
    EnsureOk();
    return std::get<1>(storage_);
  }

  T&& value() && {
    EnsureOk();
    return std::move(std::get<1>(storage_));
  }

  T& operator*() & noexcept { return std::get<1>(storage_); }
  const T& operator*() const& noexcept { return std::get<1>(storage_); }
  T&& operator*() && noexcept { return std::move(std::get<1>(storage_)); }

  T* operator->() noexcept { return &std::get<1>(storage_); }
  const T* operator->() const noexcept { return &std::get<1>(storage_); }

 private:
  static Status NormalizeStatus(Status status) {
    if (status.ok()) {
      return Status::Internal(
          "StatusOr constructed from OK status without a value");
    }
    return status;
  }

  static const Status& OkStatus() {
    static const Status ok = Status::Ok();
    return ok;
  }

  void EnsureOk() const {
    if (ok()) {
      return;
    }

    std::string message = "StatusOr has no value";
    if (!status().message().empty()) {
      message += ": ";
      message += status().message();
    }
    throw std::logic_error(message);
  }

  std::variant<Status, T> storage_;
};

}  // namespace kv_index

#endif  // KV_INDEX_STATUS_H_
