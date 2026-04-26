# 纯内存正排索引设计

## 目标

构建一个嵌入式 C++17 库，提供低延迟的纯内存正排查询：

```text
uint64_t primary_key -> structured value
```

组件面向高并发、读多写少的在线查询场景优化。Kafka upsert 被 SDK 消费并在本地发布成功后，后续查询必须立刻可见。读路径仍需保证读取完整 row：一次 lookup 只能看到旧完整 row 或新完整 row，不能看到部分写入的 value。

value schema 按 schema version 固定，但 schema 本身支持运行期热加载。schema 演进只支持新增字段和删除字段；已有字段类型不能变更，field ID 永不复用。

## 非目标

- SDK 不负责把全量 artifact 投递到在线机器。
- SDK 不暴露业务手动调用的 publish API。
- SDK 不提供索引层 delete 操作。删除由普通 upsert 字段表达，例如 `is_deleted`。
- 第一版不提供跨所有 shard 的全局多 key 快照隔离。核心语义是单 key 查询一致性。
- 第一版不针对字段扫描场景优化。核心访问模式是点查一条记录，并读取大部分或全部字段。

## 核心架构

索引按 primary key 分 shard。每个 shard 可以独立加载、压实和切换。

```text
ForwardIndex
  -> ShardDirectory
       -> C++17 atomic_load/store 保护的 shared_ptr<const ShardState>[N]

ShardState published container
  -> FullSnapshotView
  -> CompactDeltaSnapshot
  -> RealtimeDeltaAtomicTable mutable, read-optimized
  -> schema/layout handles
```

`ShardDirectory` 是一组可独立发布的 shard 指针。在 C++17 中，每个指针应通过 `std::atomic_load` 和 `std::atomic_store` 这类 free function 访问 `std::shared_ptr`，或者封装在一个小的 holder 类中。设计不依赖 `std::atomic<std::shared_ptr<T>>`，因为那是 C++20 设施。

读路径只访问一个 shard：

```text
Find(primary_key)
  -> shard_id = ShardFor(primary_key)
  -> acquire current ShardState for shard_id
  -> realtime_delta.Find(primary_key)
  -> compact_delta.Find(primary_key)
  -> full_snapshot.Find(primary_key)
  -> return ValueView
```

每条 Kafka upsert 都是完整 row。命中 delta 后即可返回完整 value，所以读路径永远不需要把 base 和 delta 的字段做合并。

## 一致性模型

索引提供单 key 快照语义：

- 一次 lookup 只观察一个 shard state。
- 在该 shard 内，lookup 看到旧完整 row 或新完整 row。
- lookup 永远不会看到部分编码完成的 row。
- 全量更新期间，不同 shard 可以在不同时间切换到新的 full artifact。

设计刻意不保证跨 shard 的多 key 请求观察同一个全局 full version。更强的保证需要全局 manifest，也会增加全量更新时的内存压力。

## 实时增量更新

Kafka 消息是整条记录 upsert：

```text
primary_key -> complete structured row
```

索引层没有 delete 操作。删除由 upsert row 中的普通字段表达。

serving 路径使用读优化的 mutable delta table：

```text
RealtimeDeltaAtomicTable
  -> sharded hash table
  -> slot: primary_key -> atomic RowRef*
  -> append-only row arena
```

upsert 发布流程：

```text
1. 解码并校验 Kafka row。
2. 把完整 row 编码进 append-only row arena。
3. 在该 shard 的 realtime table 中找到或创建 primary-key slot。
4. release-store 新 RowRef* 到该 slot。
5. 本地发布成功后再 commit Kafka offset。
```

第 4 步完成后，后续读请求即可看到新 row。旧 row memory 不在写入路径上立即回收；被替换的 row 会保留在 append-only arena 中，直到 realtime table rotate 或 compact，并且 reader epoch 证明没有查询还可能引用旧 row 后再释放。

realtime table 应避免读路径应用层锁。写入在创建 slot 时可以使用 shard-local lock 或 CAS，但读路径 common path 应是 hash lookup 加 atomic pointer load。

## 全量 Artifact 与分片

离线全量构建输出 sharded artifact：

```text
IndexArtifact
  -> Header
  -> Metadata
  -> RuntimeSchema
  -> ShardDirectory
       -> ShardArtifact[0]
       -> ShardArtifact[1]
       -> ...
       -> ShardArtifact[N-1]
```

每个 shard artifact 包含：

```text
ShardArtifact
  -> shard_id
  -> row_count
  -> PrimaryKeyEntries or frozen hash index
  -> RowArena
  -> StringPools
  -> ListPools
  -> Checksums
```

Header 和 metadata 包含：

```text
magic
format_version
artifact_id
schema_version
shard_count
build_time
source_watermark
section_directory
checksum
```

`source_watermark` 是 Kafka replay 起点的首选依据。只有当上游无法提供更强 watermark 时，才用 `build_time + safety_buffer` 作为兜底方案。

full snapshot 应尽量 mmap-friendly。row arena、string pools、list pools 和 frozen primary-key index 应尽可能直接从 artifact 读取。这样全量更新不需要两份完整数据同时 heap 常驻。

## 主键索引

full snapshot 的 artifact 应存储以下两者之一：

- 有序的 `primary_key -> row_offset` 数组。
- mmap-friendly 的 frozen hash table。

第一版可以同时支持两种模式：

```text
low-memory mode: 在有序 mmap array 上做二分查找
low-latency mode: frozen hash table view
```

`absl::flat_hash_map<uint64_t, RowOffset>` 仍适合 compact delta 或 realtime delta snapshot，因为它们的 heap 开销只由近期更新决定，而不是全量数据集。

## Row Layout 与 Value Encoding

主存储采用 row-based 布局，因为主访问模式是点查一条记录并读取大部分或全部字段。

```text
RowRecord
  -> row_size
  -> presence_bitmap
  -> fixed_area
  -> var_or_ref_area
```

scalar 字段存储在 `fixed_area` 中。string 和 list 使用字段级 encoding policy：

- `inline`：短值直接存储在 row 内。
- `dict`：row 内存 dictionary ID，真实值在 string 或 list pool 中。
- `arena`：row 内存 variable-length arena 的 offset 和 length。
- `list_dict`：对整个 list value 做去重。
- `element_dict`：对重复 list 元素做去重，尤其适合重复 string 元素。

encoding policy 按字段配置。离线 builder 也可以基于 Parquet 统计信息选择默认策略，但显式配置优先。

## 运行期 Schema

schema 是运行期对象，并包含在每个全量 artifact 中。每个 shard state 绑定一个 schema version 和 compiled row layout。

字段使用稳定的 `FieldId` 标识，而不仅仅依赖字段名：

```text
FieldId -> name, type, repeated/list flag, nullable/default, encoding, status
```

schema 演进规则：

- 新增字段会创建新的 field ID。
- 删除字段会把字段标记为 deprecated 或 tombstoned。
- field ID 永不复用。
- 字段类型变更会被拒绝。
- row 只能用它所属 schema version 的 compiled layout 解释。

客户端应在热路径外把字段名解析成 `FieldId` 或 `FieldAccessor<T>`。accessor 携带 schema-version 校验；当它被用于更新的 shard state 时，可以重新解析或安全失败。

## 在线全量更新

全量更新按 shard 执行，避免同时持有两份完整全量数据。

```text
ActiveGeneration
  -> active ShardState[0..N-1]

RebuildGeneration
  -> artifact_id
  -> full_watermark W
  -> NewShard[0..N-1]
       -> new_full: unloaded / loaded
       -> rebuild_delta_since_W
       -> replay_progress
```

需要两条逻辑更新流：

```text
Live Apply
  -> 消费当前 Kafka stream
  -> 立即写入 active serving shards
  -> 保证消费后可读

Rebuild Catch-up
  -> 从 full_watermark W 开始消费
  -> 按 shard_id 分发 upsert 到 NewShard[shard_id].rebuild_delta_since_W
  -> 在每个 shard 切换前准备好 cutover 数据
```

这两条流可以实现成两个 consumer，也可以实现成一个 catch-up consumer 加一个在追上后开始双写的 live consumer。正确性要求是：active serving shards 持续接收 live upsert，同时 rebuild shards 积累 `W` 之后的所有 upsert。

单 shard 切换流程：

```text
1. 从 sharded artifact 加载 shard_i 的 new_full。
2. 确认 shard_i 的 rebuild_delta_since_W 已追到安全位置。
3. 构建 NewShardState_i = new_full_i + rebuild_delta_i + empty realtime table。
4. 记录 shard_i 的 cutover position C。
5. 确认 rebuild_delta_i 已包含 shard_i 上所有 <= C 的消息。
6. 短暂暂停 shard_i 的 live apply，或把 > C 的 live 消息写入 per-shard handoff buffer。
7. 原子替换 active_shards[i] 为 NewShardState_i。
8. 把 handoff buffer 中 > C 的消息写入新的 realtime table。
9. 后续 shard_i 的 live upsert 路由到新的 active shard。
10. 等 reader drain 后释放旧 shard。
```

shard_i 切换瞬间：

```text
before: old_full_i + old_realtime_delta_i
after:  new_full_i + rebuild_delta_since_W_i + new realtime_delta_i
```

尚未切换的 shard 继续服务：

```text
old_full + old_realtime_delta
```

它们的 rebuild delta 会在后台继续积累，直到各自完成 cutover。

## Kafka 顺序要求

Kafka topic 最好按 `primary_key` 作为 key，保证同一个 key 的更新在一个 partition 内有序。如果无法保证，消息必须携带单调递增的业务版本或 source position。

delta 写入应保存可比较的 source position，例如：

```text
partition + offset
business_version
event_time + tie_breaker
```

这可以防止 live apply 和 rebuild catch-up 重叠时，较旧的 replay 更新覆盖较新的 live 更新。

## Delta Compaction

realtime delta 会随 live 更新增长。每个 shard 应独立 compact：

```text
RealtimeDeltaAtomicTable
  -> scan latest RowRef per key
  -> build CompactDeltaSnapshot
  -> create a fresh empty RealtimeDeltaAtomicTable
  -> atomically publish new ShardState
```

读路径层数保持固定：

```text
realtime_delta -> compact_delta -> full_snapshot
```

如果某个 shard 的 compact delta 超过配置阈值，组件应该报警或在下一次 full rebuild 中优先处理该 shard。

## 在线 API 草图

```cpp
class ForwardIndex {
 public:
  std::optional<ValueView> Find(uint64_t primary_key) const;
  std::shared_ptr<const ShardState> CurrentShard(uint64_t primary_key) const;

  LoadId LoadAsync(const LoadRequest& request);
  LoadState GetLoadState(LoadId id) const;
  bool CancelLoad(LoadId id);
};

class ShardState {
 public:
  uint32_t ShardId() const;
  uint64_t Generation() const;
  const RuntimeSchema& Schema() const;
  std::optional<ValueView> Find(uint64_t primary_key) const;
};

class ValueView {
 public:
  bool Has(FieldId field_id) const;

  template <typename T>
  std::optional<T> Get(FieldId field_id) const;

  template <typename T>
  std::optional<T> Get(FieldAccessor<T> accessor) const;

  template <typename T>
  ListView<T> GetList(FieldId field_id) const;
};
```

`ValueView` 必须持有 pinned shard state，保证 row arena 和 dictionary pool 中的引用在 view 生命周期内始终有效。

## 并发模型

- 查询线程读取一个不可变 shard state 及其读优化 realtime delta。
- realtime delta 使用 atomic row pointer publication 保证即时可见。
- full rebuild 每次只加载和切换一个 shard 或一小批 shard。
- 只要还有查询持有 shared reference，旧 shard state 就会继续存活。
- 同一时刻应该只允许一个 active full rebuild generation。
- per-shard cutover、live apply routing 和 rebuild catch-up 必须由一个 update coordinator 协调。

## 内存模型

全量更新峰值内存由 shard batch size 控制，而不是由完整数据集大小控制：

```text
active full dataset
+ currently loaded new full shard batch
+ active realtime deltas
+ rebuild delta since watermark
+ compact deltas
```

为了保证内存可控：

- full shard artifact 尽量使用 mmap-backed。
- 限制 full cutover batch size。
- 按 shard 追踪 rebuild delta size。
- 如果 rebuild delta 超过配置限制，abort 或 throttle rebuild。
- full shard 优先使用 frozen index，避免重建全量级 heap hash map。

## 错误处理

load、replay、compaction 或 shard cutover 必须 fail closed。任一步骤失败时，当前 serving shard 保持不变。

失败示例：

- artifact checksum 不匹配。
- format version 不支持。
- schema 类型不兼容。
- row arena 或 dictionary section 损坏。
- rebuild catch-up 无法追到安全 cutover position。
- delta row schema 校验失败。
- Kafka ordering metadata 缺失或不一致。

运行状态应暴露 active artifact ID、per-shard generation、rebuild progress、Kafka lag、delta sizes、schema version 和 last error。

## 测试策略

核心测试：

- 从 full shard snapshot 查询能返回完整 row。
- realtime delta 本地发布后立刻可见。
- delta hit 返回整条 row，永远不和 full 做部分字段合并。
- compact delta 覆盖 full snapshot。
- realtime delta 覆盖 compact delta。
- 单 shard cutover 不影响其他 shard。
- 尚未 cutover 的 shard 仍服务 old full 加 live realtime updates。
- cutover 前积累的 rebuild delta 在 shard 切换后可见。
- 较旧的 replay update 不能覆盖较新的 live update。
- 业务删除 row 会作为带 delete marker 字段的普通 row 返回。
- schema 新增字段时，旧 snapshot 返回默认值，新 snapshot 返回实际值。
- schema 删除字段时，field ID tombstone 且永不复用。
- 已有 field ID 的类型变更会被拒绝。
- dictionary string/list 字段能正确解码。
- artifact checksum 和 format validation fail closed。
- delta compaction 后 lookup 结果保持不变。

性能测试：

- 并发 reader 下的单 key 查询延迟。
- 常见 schema 的 full-row decode 延迟。
- realtime delta lookup 延迟和 update publication 成本。
- mmap full shards、dictionary pools、realtime deltas 下的内存使用。
- per-shard load 和 cutover 时间。
- delta compaction CPU 成本。

## 待决问题

- `RowRecord` 的精确二进制编码、对齐方式和字节序。
- 第一版支持的 scalar 类型和 list element 类型集合。
- shard count 和 shard assignment function。
- full shard primary-key index 第一版只用 sorted array，还是直接支持 frozen hash table。
- realtime delta compaction、rebuild delta limit、shard cutover batch size 的默认阈值。
- field accessor 在跨 schema version 时是自动重新解析还是 fail fast。
- 第一版 full artifact 是否强制 mmap，还是配置化。
