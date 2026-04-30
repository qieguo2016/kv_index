# AGENTS.md

## 项目介绍

`kv_index` 是一个用 C++20 构建的嵌入式纯内存正排索引库，面向高并发、读多写少的在线点查场景。

## 技术栈

- 构建系统：Bazel，作为唯一编译和测试入口。
- 语言标准：C++20。
- 基础库策略：优先使用 C++ 标准库中的同类组件；允许使用 Abseil (`absl`)，但只在标准库不能满足需求、性能/内存语义收益明确，或设计明确要求时使用。
- Kafka 客户端：`librdkafka`，由 `KafkaUpdateConsumer` 封装 Kafka poll、seek、lag 和 offset commit 语义。

## 文档链接

- [中文设计文档](docs/superpowers/specs/2026-04-26-in-memory-forward-index-design.zh.md)
- [English design document](docs/superpowers/specs/2026-04-26-in-memory-forward-index-design.md)
- [实现计划](docs/superpowers/plans/2026-04-27-in-memory-forward-index-implementation.md)

## 目录结构

```text
.
├── AGENTS.md
├── docs/
│   └── superpowers/
│       ├── plans/   # implementation plans
│       └── specs/   # design specifications
├── .worktrees/      # local/untracked worktree scratch space
└── bazel-*          # Bazel-generated symlinks, not source files
```
