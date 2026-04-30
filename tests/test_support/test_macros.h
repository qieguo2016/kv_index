#ifndef KV_INDEX_TEST_SUPPORT_TEST_MACROS_H_
#define KV_INDEX_TEST_SUPPORT_TEST_MACROS_H_

#include <cstdlib>
#include <exception>
#include <iostream>
#include <string_view>

#define KV_INDEX_CHECK(condition)                                             \
  do {                                                                        \
    if (!(condition)) {                                                       \
      std::cerr << __FILE__ << ":" << __LINE__ << ": check failed: "         \
                << #condition << std::endl;                                   \
      std::abort();                                                           \
    }                                                                         \
  } while (false)

#define KV_INDEX_CHECK_EQ(actual, expected)                                   \
  do {                                                                        \
    const auto kv_index_actual = (actual);                                    \
    const auto kv_index_expected = (expected);                                \
    if (!(kv_index_actual == kv_index_expected)) {                            \
      std::cerr << __FILE__ << ":" << __LINE__ << ": check failed: "         \
                << #actual << " == " << #expected << std::endl;              \
      std::abort();                                                           \
    }                                                                         \
  } while (false)

#define KV_INDEX_CHECK_NE(actual, expected)                                   \
  do {                                                                        \
    const auto kv_index_actual = (actual);                                    \
    const auto kv_index_expected = (expected);                                \
    if (!(kv_index_actual != kv_index_expected)) {                            \
      std::cerr << __FILE__ << ":" << __LINE__ << ": check failed: "         \
                << #actual << " != " << #expected << std::endl;              \
      std::abort();                                                           \
    }                                                                         \
  } while (false)

#define KV_INDEX_CHECK_LT(actual, expected)                                   \
  do {                                                                        \
    const auto kv_index_actual = (actual);                                    \
    const auto kv_index_expected = (expected);                                \
    if (!(kv_index_actual < kv_index_expected)) {                             \
      std::cerr << __FILE__ << ":" << __LINE__ << ": check failed: "         \
                << #actual << " < " << #expected << std::endl;               \
      std::abort();                                                           \
    }                                                                         \
  } while (false)

#define KV_INDEX_CHECK_THROWS(statement, exception_type)                      \
  do {                                                                        \
    bool kv_index_threw_expected = false;                                     \
    try {                                                                     \
      statement;                                                              \
    } catch (const exception_type&) {                                         \
      kv_index_threw_expected = true;                                         \
    } catch (const std::exception& e) {                                       \
      std::cerr << __FILE__ << ":" << __LINE__ << ": expected "              \
                << #exception_type << ", got exception: " << e.what()         \
                << std::endl;                                                \
      std::abort();                                                           \
    } catch (...) {                                                           \
      std::cerr << __FILE__ << ":" << __LINE__ << ": expected "              \
                << #exception_type << ", got unknown exception" << std::endl; \
      std::abort();                                                           \
    }                                                                         \
    if (!kv_index_threw_expected) {                                           \
      std::cerr << __FILE__ << ":" << __LINE__ << ": expected exception: "   \
                << #exception_type << std::endl;                              \
      std::abort();                                                           \
    }                                                                         \
  } while (false)

#endif  // KV_INDEX_TEST_SUPPORT_TEST_MACROS_H_
