# 纯内存正排索引设计

## 目标

构建一个嵌入式 C++20 库，提供低延迟的纯内存正排查询：

```text
uint64_t primary_key -> structured value
```

组件面向高并发、读多写少的在线查询场景优化。Kafka upsert 被目标 shard 的 active generation update stream 本地发布成功后，后续查询必须立刻可见。读路径必须读取一个完整 row version，不能看到部分写入的 value。

value schema 按 schema version 固定，但 schema 本身支持运行期热加载。schema 演进只支持新增字段和删除字段；已有字段类型不能变更，field ID 永不复用。

## 非目标

- SDK 不负责把全量 artifact 投递到在线机器。
- SDK 不暴露业务手动调用的 publish API。
- SDK 不提供索引层 delete 操作。删除由普通 upsert 字段表达，例如 `is_deleted`。
- 第一版不提供跨所有 shard 的全局多 key 快照隔离。核心语义是单 key 查询一致性。
- 第一版不针对字段扫描场景优化。核心访问模式是点查一条记录，并读取全部字段。

## 在线 API 草图

```cpp
struct ForwardIndexOptions {
  uint32_t shard_count = 128;
  ThresholdConfig thresholds;
  KafkaConsumerConfig kafka_consumer;
};

class ForwardIndex {
 public:
  explicit ForwardIndex(const ForwardIndexOptions& options);

  std::optional<Row> Get(uint64_t primary_key) const;
  std::vector<std::optional<Row>> MGet(const std::vector<uint64_t>& primary_keys) const;

  LoadId LoadAsync(const LoadRequest& request);
  LoadState GetLoadState(LoadId id) const;
  bool CancelLoad(LoadId id);
};

class ShardState {
 public:
  uint32_t ShardId() const;
  uint64_t Generation() const;
  const RuntimeSchema& Schema() const;
  std::optional<Row> Get(uint64_t primary_key) const;
  std::vector<std::optional<Row>> MGet(const std::vector<uint64_t>& primary_keys) const;
};

class Row {
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

`ForwardIndexOptions` 是 SDK 初始化入口，集中配置 shard count、delta compaction / full rebase / rebuild cutover 阈值，以及 Kafka consumer 参数。`ForwardIndex` 构造后持有这些配置，并用同一套配置创建内部 `KafkaUpdateConsumer` 和后台更新 coordinator。

`ShardState` 是内部类型；public query API 不暴露 `CurrentShard`。`Row` 必须持有 pinned shard state，保证 row arena 和 dictionary pool 中的引用在 row 生命周期内始终有效。scalar field 读取总是 copy 出去；string/list 字段可以按 API 选择返回 owning copy 或 pinned view。view 引用的生命周期不能超过持有 backing pin 的 `Row` 生命周期。`ForwardIndex::MGet` 的返回结果与输入 keys 保持相同顺序，内部按 shard 对 key 分组，并对每个 shard 调用 `ShardState::MGet`，避免重复 acquire 同一个 shard pointer。`ShardState::MGet` 只处理已经属于该 shard 的 keys，并保持传入 shard-local keys 的顺序。

## 一致性模型

索引提供单 key 快照语义：

- 一次 lookup 只观察一个 shard state。
- 在该 shard 内，lookup 看到一个完整 row version。
- lookup 永远不会看到部分编码完成的 row。
- 全量更新期间，不同 shard 可以在不同时间切换到新的 full artifact。

设计刻意不保证跨 shard 的多 key 请求观察同一个全局 full version。更强的保证需要全局 manifest，也会增加全量更新时的内存压力。

## 核心架构

索引按 primary key 分 shard。每个 shard 可以独立加载、压实和切换。

第一版默认 shard count 为 `128`。在读多写少、全量数据约 100 GiB、约 100M rows 的工况下，平均每个 shard 约 781K rows、约 800 MiB full data。这个粒度避免 shard metadata、artifact section、状态指标和 cutover 调度过碎，同时仍可按 shard 控制 mmap load、cutover 和 compaction 内存峰值。

shard assignment 使用稳定、版本化的 64-bit hash，不使用可能随进程或 Abseil 版本变化的 `absl::Hash`：

```text
hash = StableHash64(primary_key, hash_seed, hash_version)
shard_id = hash & (shard_count - 1)
```

第一版要求 `shard_count` 为 2 的幂。`hash_seed` 和 `hash_version` 写入 artifact metadata，保证离线构建、在线 serving 和 rebuild catch-up 使用同一分片函数。

```text
ForwardIndex
  -> ShardDirectory
       -> std::atomic<std::shared_ptr<const ShardState>>[N]

ShardState published container
  -> FullSnapshotView
       -> ImmutableRowSnapshotView mmap-backed or rebase-owned
  -> CompactDeltaSnapshot
       -> ImmutableRowSnapshotView owned heap-backed
  -> RealtimeDeltaAtomicTable mutable, read-optimized
  -> schema/layout handles
```

`ShardDirectory` 是一组可独立发布的 shard 指针。在 C++20 中，每个指针表示为 `std::atomic<std::shared_ptr<const ShardState>>`。发布 shard 使用 release-store；读线程使用 acquire-load 在 lookup 前 pin 住 shard state。

读路径只访问一个 shard：

```text
Get(primary_key)
  -> shard_id = ShardFor(primary_key)
  -> acquire current ShardState for shard_id
  -> realtime_delta.Get(primary_key)
  -> compact_delta.Get(primary_key)
  -> full_snapshot.Get(primary_key)
  -> return Row
```

每条 upsert 都是完整 row。命中 delta 后即可返回完整 value，所以读路径永远不需要把 base 和 delta 的字段做合并。

`FullSnapshotView` 和 `CompactDeltaSnapshot` 都是薄 wrapper，读侧都委托给 `ImmutableRowSnapshotView`。两者使用相同的 primary-key lookup、row decode、string/list pool 解析和 schema/layout 校验逻辑；差异只在 backing memory 和附加 metadata。外部 `AsyncLoad` 发布的 full snapshot 使用 mmap-backed artifact；内部 `Full Rebase` 生成的 full snapshot 可以使用 owned backing。

所有 shard 使用相同的 published container 结构。delta compaction、内部 `Full Rebase` 和外部 `AsyncLoad` 也使用同一类 generation 切换模型：

```text
1. 创建 NewGeneration / NewShardState。
2. Kafka upsert 从切换起点开始同时写入当前 serving generation 和 new generation。
3. 后台只扫描切换起点之前已经不可变的输入，构建 new generation 的 compact/full 层。
4. 确认 new generation 的实时增量已经追上当前 serving generation。
5. 原子切换 active_shards[i]，读路径开始读取 new generation。
6. 等 reader drain 后下线旧 generation。
```

因此读路径始终保持：

```text
realtime_delta -> compact_delta -> full_snapshot
```

区别只在后台构建 new generation 时扫描哪些不可变输入。

## 运行期 Schema

schema 是运行期对象，并包含在每个全量 artifact 中。每个 shard state 绑定一个 schema version 和 compiled row layout。

字段使用稳定的 `FieldId` 标识，而不仅仅依赖字段名：

```text
FieldId -> name, type, repeated/list flag, nullable/default, encoding, status
```

schema 演进规则：

- 新增字段会创建新的 field ID。
- 删除字段会把字段标记为 deleted。
- field ID 永不复用。
- 字段类型变更会被拒绝。
- row 只能用它所属 schema version 的 compiled layout 解释。

外部 `AsyncLoad` 切换 schema/layout 时，旧 active generation 仍按旧 schema 解码 Kafka row：新增字段会被忽略，已删除字段仍按旧 layout 处理直到该 shard 切走。new rebuild generation 按新 schema 编码 Kafka row；删除字段按照新 layout 处理，不再写入新的 row slot。每个 shard 的 row 必须始终用其所属 generation 绑定的 schema/layout 解释。

客户端应在热路径外把字段名解析成 `FieldId` 和 `FieldAccessor<T>`。accessor 携带 schema-version 校验；当它被用于不兼容的 shard state 时 fail fast。

支持的 scalar 类型：

```text
int8
int32
int64
uint64
bool
string
```

第一版支持的 list 类型：

```text
list<int8>
list<int32>
list<int64>
list<uint64>
list<bool>
list<string>
```

`presence_bitmap` 区分 missing/null 与 present。list 字段 present 且 `element_count = 0` 表示空 list；字段 not present 表示 missing/null/default。

`FieldAccessor<T>` 携带：

```text
schema_version
layout_fingerprint
field_id
physical_type
is_list
nullable/default policy
field_offset/ref_offset
```

`Row::Get(accessor)` 必须先比较 `schema_version` 和 `layout_fingerprint`。不兼容时不能尝试按 offset 读取，也不能 fallback 到 field name 查找。第一版策略：

- debug/test build 使用 `CHECK`/assert fail fast。
- release build 返回 accessor mismatch error，并增加 `field_accessor_mismatch_total` 指标。
- 如果 public API 保持 `std::optional<T>`，则内部必须记录 last error 或提供可观测 status，避免把 accessor mismatch 静默伪装成字段缺失。

## 实时增量更新

Kafka 消息是整条记录 upsert：

```text
primary_key -> complete structured row
```

索引层没有 delete 操作。删除由 upsert row 中的普通字段表达。

serving 路径使用读优化的 mutable delta table：

```text
RealtimeDeltaAtomicTable
  -> fixed-capacity RealtimeAtomicHashMap
  -> slot: atomic key state + primary_key + atomic RowRef*
  -> append-only row arena
  -> append-only string/list payload pools
```

upsert 发布流程：

```text
1. 按目标 generation 的 schema/layout 解码并校验 Kafka row。
2. 把完整 row 编码为一个不可变 RowRef：fixed row slot 写入 row arena，
   string/list payload 写入该 realtime generation 的 append-only payload pools，
   row slot 内只保存固定宽度引用。
3. 在该 shard 的 realtime table 中查找 primary-key slot；slot 不存在时，通过 CAS 创建。
4. release-store 新 RowRef* 到该 slot。
5. 本地发布成功后再 commit Kafka offset。
```

第 4 步完成后，后续读请求即可看到新 row。旧 row memory 不在写入路径上立即回收；被替换的 row 会保留在 append-only arena 中，直到 realtime table generation 完成 compaction，并且 reader epoch 证明没有查询还可能引用旧 row 后再释放。

realtime table 避免读路径应用层锁。slot 创建使用 CAS 更新 slot key state；读路径 common path 是 open-addressing probe 加 atomic pointer load。hash table 的具体设计见 `RealtimeAtomicHashMap`。

## Kafka Consumer 模块

第一版只支持 Kafka，不引入可插拔数据源抽象。Kafka consumer 相关能力收敛到独立模块，避免 Kafka poll、seek、lag 和 offset commit 逻辑散落在索引更新流程中。该模块不包含索引逻辑：不解析 schema、不选择 generation、不写 realtime delta、不执行 shard cutover。

层次关系：

```text
ForwardIndex SDK
  -> UpdateCoordinator
       -> KafkaUpdateConsumer
       -> UpdateApplier
       -> ShardCutoverCoordinator
```

`KafkaUpdateConsumer` 负责创建和持有 Kafka consumer、按 partition/offset seek、poll batch、暴露 partition lag/progress，并在 SDK 确认本地发布成功后 commit offset。`UpdateCoordinator` 调用 consumer 拉取 batch，再把消息交给 `UpdateApplier`；`UpdateApplier` 按目标 generation 绑定的 schema/layout 解码 row 并写 realtime delta。这样外部 `AsyncLoad` 双写窗口内，同一条 Kafka message 仍可以分别按旧 active generation layout 和新 rebuild generation layout 编码。

建议接口：

```cpp
struct KafkaPartition {
  std::string topic;
  int32_t partition;
};

struct KafkaPosition {
  KafkaPartition partition;
  uint64_t offset;
};

struct KafkaUpsertMessage {
  uint64_t primary_key;
  KafkaPosition position;
  absl::Cord raw_payload;
  KafkaMessageMetadata metadata;
};

struct KafkaCheckpoint {
  absl::flat_hash_map<KafkaPartition, KafkaPosition> positions;
};

class KafkaUpdateConsumer {
 public:
  StatusOr<std::vector<KafkaUpsertMessage>> PollBatch(PollOptions options);
  Status Commit(const KafkaCheckpoint& checkpoint);
  Status Seek(const KafkaCheckpoint& checkpoint);
  StatusOr<KafkaProgress> GetProgress() const;
};
```

`UpdateCoordinator` 负责循环 `PollBatch`，把 message 交给 `UpdateApplier` 写入目标 generation set，并且只在 batch 内所有 message 都本地发布成功后 commit 对应 Kafka offset。失败时 fail closed，不推进 offset。测试中可以用 fake Kafka client 或 recorded `KafkaUpsertMessage` batch 验证 coordinator 与 consumer 的交互，不需要引入非 Kafka 数据源接口。

## 在线全量更新

`FullSnapshotView` 有两个更新触发路径：

- 外部 `AsyncLoad`：离线系统构建好新的 sharded full artifact 后，外部调用 `ForwardIndex::LoadAsync`。该路径可以切换 artifact、schema version 和 compiled layout。
- 内部 `Full Rebase`：当某个 shard 的 `CompactDeltaSnapshot` 体积过大时，组件内部把该 compact snapshot 合并进该 shard 的 `FullSnapshotView`。该路径不依赖外部 artifact，不改变 schema/layout，只降低 serving 层级中的 compact 压力。

两条路径都按 shard 执行，避免同时持有两份完整全量数据。

### 外部 AsyncLoad

当离线流程产出了全量索引之后，SDK集成方调用SDK的AsyncLoad接口触发全量更新。

```text
ActiveGeneration
  -> active ShardState[0..N-1]

RebuildGeneration
  -> artifact_id
  -> full_watermark W
  -> NewShard[0..N-1]
       -> new_full: unloaded / loaded
       -> RealtimeDeltaAtomicTable
       -> replay_progress
```

需要两条逻辑更新流：

```text
Live Apply
  -> 消费当前 Kafka stream
  -> 立即写入 ActiveGeneration shards
  -> 对仍指向 ActiveGeneration 的 shard 作为 authoritative visible stream

Rebuild Catch-up
  -> 从 full_watermark W 开始消费
  -> 按 RebuildGeneration 绑定的 schema/layout 写入 RebuildGeneration shards
  -> shard 切换到 RebuildGeneration 后，作为该 shard 的 authoritative visible stream
```

这两条流实现成两个独立 `KafkaUpdateConsumer`。rebuild 期间，两个 consumer 都保持全局运行。Live Apply 持续写 ActiveGeneration 的所有 shard，直到整个 rebuild 完成。Rebuild Catch-up 从 watermark `W` 开始持续写 RebuildGeneration 的所有 shard，直到所有 shard 完成切换。某个 shard 切换后，Rebuild Catch-up 成为该 shard 的 active generation update stream；Live Apply 写入旧 ActiveGeneration shard 的数据不再 serving-visible。

如果新 full artifact 携带新的 schema version，Live Apply 仍按旧 active generation schema 编码；Rebuild Catch-up 按新 rebuild generation schema 编码。这样同一条 Kafka upsert 在双写窗口内可以进入两个不同 layout 的 row store，但每个 generation 内部仍只用自己的 compiled layout 解释 row。

单 shard 切换流程：

```text
1. 从 sharded artifact 加载 shard_i 的 new_full。
2. 确认 RebuildGeneration shard_i 相关的所有 Kafka partitions lag 都低于配置阈值，并且已追过 Live Apply 为 shard_i 发布过的安全 source position。
3. 把 new_full_i 绑定到 RebuildGeneration shard_i。
4. 原子替换 active_shards[i] 为 RebuildGeneration shard_i。
5. 两个 `KafkaUpdateConsumer` 继续保持全局运行。
6. 等 reader drain 后释放旧 shard。
```

shard_i 切换瞬间：

```text
before: old_full_i + old_realtime_delta_i
after:  new_full_i + rebuild_realtime_delta_i
```

尚未切换的 shard 继续服务：

```text
old_full + old_realtime_delta
```

它们的 RebuildGeneration realtime delta 会在后台继续积累，直到各自完成 cutover。

shard 切换后，该 shard 的查询读取 RebuildGeneration。Live Apply 继续全局消费并写入 ActiveGeneration，直到整个 rebuild 完成，但这些写入对已切换 shard 不再 serving-visible。Rebuild Catch-up 是已切换 shard 的 serving-visible stream，并且必须按照配置的 per-partition lag threshold 保持追平。cutover coordinator 需要按 Kafka partition 追踪 lag；只有该 shard 相关的所有 partitions 都低于阈值时，才能认为 rebuild catch-up 已经追上。

shard cutover batch 默认：

- 默认每次 cutover `1` 个 shard。
- 允许配置为最多 `2` 个 shard，但要求当前加载的新 full shard batch 总 bytes <= 2 GiB。
- 如果机器可用内存低于配置水位，batch size 自动降为 `1`。

### 内部 Full Rebase

内部 `Full Rebase` 用于控制 compact delta 长期增长。它只处理单个 shard，并把该 shard 当前 sealed 的 `CompactDeltaSnapshot` 合并进 `FullSnapshotView`。它仍遵循 generation 切换模型：rebase 期间 Kafka upsert 同时写入旧 serving generation 和 rebase generation；后台只扫描不可变的 compact/full 输入；rebase generation 追平后再切读。

```text
before: full_snapshot + compact_delta + realtime_delta
after:  rebased_full_snapshot + empty compact_delta + rebase_realtime_delta
```

rebase 流程：

```text
1. pin 当前 serving ShardState。
2. 创建 rebase generation，包含 fresh empty RealtimeDeltaAtomicTable 和 empty CompactDeltaSnapshot。
3. 从切换起点开始，Kafka upsert 同时写入当前 serving generation 和 rebase generation。
4. 确认切换起点之前的 realtime 数据已经通过 delta compaction 进入 sealed CompactDeltaSnapshot；否则先完成一次 delta compaction。
5. 先扫描 CompactDeltaSnapshot，再扫描 FullSnapshotView；key 冲突时 compact row 覆盖 full row。
6. 把合并后的完整 shard 写成新的 owned/mmap-capable full snapshot backing。
7. 确认 rebase generation 的 realtime delta 已追上当前 serving generation。
8. 原子替换 active_shards[i] 为 rebased ShardState。
9. 等 reader drain 后释放旧 full、旧 compact 和旧 realtime generation。
```

`Full Rebase` 不消费外部 full watermark，也不切换 schema version。rebase 期间的新 Kafka upsert 写入 rebase generation 的 realtime delta，并在切换后作为最高优先级覆盖层。发布后读路径仍保持：

```text
realtime_delta -> compact_delta -> full_snapshot
```

内部触发阈值按 shard 计算，满足任一条件即触发：

- compact snapshot bytes >= full shard bytes 的 10%。
- compact snapshot bytes >= 128 MiB。
- compact snapshot unique keys >= shard row count 的 10%。

同一 shard 同一时刻只允许一个 rebase；外部 `AsyncLoad` 与内部 `Full Rebase` 冲突时，外部 `AsyncLoad` 优先，内部 rebase 取消。

## Kafka 顺序要求

Kafka topic 必须按 `primary_key` 作为 key，保证同一个 key 的更新在一个 partition 内有序。每条消息同时携带可比较的 source position。

delta 写入应保存可比较的 source position，例如：

```text
partition + offset
business_version
event_time + tie_breaker
```

这可以防止同一个 generation 内较旧的 source position 覆盖较新的 source position。

## Delta Compaction

realtime delta 会随 live 更新增长。每个 shard 应独立 compact：

```text
ActiveGeneration
  -> full_snapshot + compact_delta + realtime_delta

CompactGeneration
  -> same full_snapshot
  -> rebuilt compact_delta
  -> fresh realtime_delta
```

delta compaction 也使用双写、切读、下线旧 generation 的流程：

```text
1. 记录 active realtime append-only buffer 的当前 offset 作为 compact boundary。
2. 创建 CompactGeneration，包含 fresh empty RealtimeDeltaAtomicTable。
3. 从 boundary 之后开始，upsert 同时写入 ActiveGeneration 和 CompactGeneration。
4. 从 active realtime append-only buffer 的 boundary 向前扫描到开头；同一个 key 只保留第一个遇到的最新 RowRef。
5. 继续扫描旧 CompactDeltaSnapshot；只补充尚未被 realtime scan 覆盖的 key。
6. SnapshotBuilder 构建 owned ImmutableRowSnapshot，作为新的 CompactDeltaSnapshot。
7. 确认 CompactGeneration 的 realtime delta 已追上 ActiveGeneration。
8. 原子替换 active_shards[i] 为 CompactGeneration。
9. 等 reader drain 后释放旧 realtime generation 和旧 compact snapshot。
```

`CompactDeltaSnapshot` 按 primary key 存储自 full snapshot 以来发生变更记录的完整更新后 row slot。新的 compact snapshot 等价于 `sealed realtime delta + old compact snapshot`；key 冲突时 sealed realtime row 覆盖 old compact row。它与 `FullSnapshotView` 使用同一个 `ImmutableRowSnapshotView` 读接口；`FullSnapshotView` 的 backing 可以来自 mmap artifact 或内部 rebase 生成的 owned snapshot，`CompactDeltaSnapshot` 的 backing 来自 compaction 生成的 owned heap snapshot。

读路径层数保持固定：

```text
realtime_delta -> compact_delta -> full_snapshot
```

如果某个 shard 的 compact delta 超过配置阈值，组件触发内部 `Full Rebase`，把 compact delta 合并进该 shard 的 `FullSnapshotView`。

默认 realtime delta compaction 触发条件按 shard 计算，满足任一条件即触发：

- `RealtimeAtomicHashMap` load factor >= 0.60。
- realtime delta unique keys >= shard row count 的 5%。
- realtime delta row arena bytes >= 64 MiB。
- realtime delta append-only payload pools bytes >= 64 MiB。

对默认 128 shards，这大约对应每 shard 约 39K unique updated keys，通常远小于 full shard 大小，可以保持 realtime probe 短、compaction 成本可控。`CompactDeltaSnapshot` 继续增长到更高阈值后，由内部 `Full Rebase` 合并进 `FullSnapshotView`。

## 并发模型

- 查询线程读取一个不可变 shard state 及其读优化 realtime delta。
- realtime delta 使用 CAS slot creation 和 atomic row pointer publication 保证即时可见。
- 外部 full rebuild 和内部 full rebase 每次默认只加载和切换一个 shard。
- 只要还有查询持有 shared reference，旧 shard state 就会继续存活。
- 同一时刻应该只允许一个 active external full rebuild generation。
- 同一 shard 同一时刻只允许一个 internal full rebase；它不能与该 shard 的 external cutover 并发。
- per-shard cutover 和 rebuild catch-up 必须由一个 update coordinator 协调。

## 内存模型

全量更新峰值内存由 shard batch size 控制，而不是由完整数据集大小控制：

```text
active full dataset
+ currently loaded new full shard batch
+ active realtime deltas
+ compaction/rebase generation realtime deltas during double-write windows
+ rebuild generation realtime deltas since watermark
+ compact deltas
```

为了保证内存可控：

- full shard artifact 尽量使用 mmap-backed。
- 限制 full cutover batch size。
- 按 shard 追踪 rebuild generation realtime delta size。
- 如果 rebuild delta 超过配置限制，abort rebuild。
- full shard 优先使用 frozen index，避免重建全量级 heap hash map。

## 错误处理

load、replay、compaction 和 shard cutover 必须 fail closed。任一步骤失败时，当前 serving shard 保持不变。

失败示例：

- artifact checksum 不匹配。
- format version 不支持。
- schema 类型不兼容。
- row arena section 损坏。
- dictionary section 损坏。
- Rebuild Catch-up 无法追到安全 cutover position。
- delta row schema 校验失败。
- Kafka ordering metadata 无效。

运行状态应暴露 active artifact ID、per-shard generation、rebuild progress、Kafka lag、delta sizes、schema version 和 last error。

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
  -> FrozenPrimaryKeyIndex
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

第一版 full artifact 强制 mmap：

- 外部 full artifact 必须通过 read-only mmap 加载，不提供 full dataset heap load 模式。
- mmap 使用 private/read-only mapping；row arena、string pools、list pools 和 frozen primary-key index 均从 artifact section 直接读取。
- loader 必须校验 header、format version、section directory、schema compatibility 和 checksum；失败时 fail closed，当前 serving shard 不变。
- 内部 `Full Rebase` 和 compact delta snapshot 可以使用 owned heap backing，但 seal 后必须暴露与 mmap full snapshot 相同的 `ImmutableRowSnapshotView`。

## 主键索引

full snapshot 和 compact delta snapshot 在 serving 读路径中都使用 frozen primary-key index view：

```text
FrozenPrimaryKeyIndexView: primary_key -> row_offset
```

full snapshot 的 index 从 mmap artifact 读取；compact delta snapshot 的 index 由 compaction 生成并由 heap backing 持有。两者对读路径暴露相同的 lookup API。`absl::flat_hash_map<uint64_t, RowOffset>` 仍适合后台构建阶段的临时索引，但 serving-visible 的 compact snapshot 应在 seal 后转换成 `FrozenPrimaryKeyIndexView`，以复用 full snapshot 的读路径。serving-visible 的 realtime delta 使用下面定义的 `RealtimeAtomicHashMap`。

`FrozenPrimaryKeyIndexView` 在 mmap-backed full snapshot 和 owned-backed compact snapshot 中使用相同二进制布局：

```text
FrozenPrimaryKeyIndex
  -> Header
       magic
       format_version
       row_count
       capacity
       hash_seed
       hash_version
       group_width
       control_offset
       key_offset
       row_offset_offset
  -> control_bytes[capacity + group_width]
  -> keys[capacity] uint64_t
  -> row_offsets[capacity] uint64_t
```

- index 使用 SwissTable-style frozen open addressing。
- `capacity` 按 `row_count / 0.875` 向上取整到 2 的幂。
- `control_bytes` 存 H2 metadata 和 empty sentinel；没有 tombstone，因为 snapshot immutable。
- `keys` 存 primary key，`row_offsets` 存 `RowArenaView` 内的 byte offset。
- mmap-backed 直接指向 artifact section；owned-backed 持有同样布局的一段 heap bytes。读路径只依赖 view，不关心 backing 类型。

## ImmutableRowSnapshotView

`ImmutableRowSnapshotView` 是 full snapshot 和 compact delta snapshot 的共享读侧结构：

```text
ImmutableRowSnapshotView
  -> FrozenPrimaryKeyIndexView
  -> RowArenaView
  -> StringPoolView
  -> ListPoolView
  -> RuntimeSchema
  -> CompiledRowLayout
  -> SnapshotBackingRef
```

它只提供 immutable lookup 和 row decode，不负责构建、压实或内存释放：

```text
Get(primary_key)
  -> FrozenPrimaryKeyIndexView.Lookup(primary_key)
  -> RowArenaView.RowAt(row_offset)
  -> construct Row with schema/layout and backing pin
```

`SnapshotBackingRef` 负责保证 row arena、string pools、list pools 和 index memory 在 `Row` 生命周期内有效。实现上提供两种 backing：

```text
MmapSnapshotBacking
  -> artifact fd / mapping
  -> section directory
  -> checksums and artifact metadata

OwnedSnapshotBacking
  -> owned frozen index bytes
  -> owned row arena
  -> owned string/list payload pools
  -> compact generation metadata
```

`FullSnapshotView` 是 `ImmutableRowSnapshotView + artifact/rebase metadata/source watermark` 的薄 wrapper。外部 `AsyncLoad` 的 full snapshot 使用 `MmapSnapshotBacking`；内部 `Full Rebase` 的 full snapshot 可以使用 `OwnedSnapshotBacking`。`CompactDeltaSnapshot` 是 `ImmutableRowSnapshotView + compact generation metadata/build stats` 的薄 wrapper。这样 full 和 compact 的二进制 encoding 可以保持一致，lookup、row decode、schema evolution 和 dictionary/list 解析测试也可以共用同一套用例。

`SnapshotBuilder` 可以在构建阶段使用 `absl::flat_hash_map`、vector sort 或其他临时结构收集 key 到 row offset 的映射；一旦 seal 成 `OwnedSnapshotBacking`，serving 读路径只看到 frozen index view。

## 统一 Row Storage 抽象

full、compact 和 realtime 三层都使用 index 与数据分离的 row store 抽象：

```text
PrimaryKeyIndex
  -> primary_key -> RowLocator

RowStorageView
  -> RowLocator -> FixedSizeRowSlot
  -> ValueRef16 -> string/list payload pools

RowDecoder
  -> RuntimeSchema + CompiledRowLayout + FixedSizeRowSlot + payload pools
  -> Row
```

三层的差异只在 index 是否可变、backing 如何拥有内存：

```text
FullSnapshotView
  -> FrozenPrimaryKeyIndexView
  -> ImmutableRowSnapshotView

CompactDeltaSnapshot
  -> FrozenPrimaryKeyIndexView
  -> ImmutableRowSnapshotView

RealtimeDeltaAtomicTable
  -> RealtimeAtomicHashMap
  -> RealtimeRowStorageView
```

`RealtimeAtomicHashMap` serving-visible 的 value 是 atomic `RowRef*`，但 `RowRef` 语义上仍是一个 `RowLocator`：它定位 append-only row arena 中已经完整编码的 fixed row slot，并通过该 slot 内的 `ValueRef16` 解析 append-only string/list payload pools。读路径命中任意一层后，都使用同一套 schema/layout 校验、row decode 和 field accessor 逻辑；差异只在 full/compact 的 locator 是 frozen `row_offset`，realtime 的 locator 是 atomic 发布的 `RowRef*`。

## RealtimeAtomicHashMap

`RealtimeAtomicHashMap` 是 `RealtimeDeltaAtomicTable` 的主键索引。它面向 read-heavy、write-light 的在线增量层，目标不是替代通用 hash map，而是在固定容量、无 erase、generation 级回收的约束下，提供无应用层读锁的稳定查询路径。

实现应基于 Abseil SwissTable / `absl::flat_hash_map` 的成熟设计：

- 优先复用 Abseil 暴露的 public API、hash policy、hash mixing 和等价性比较语义。
- 对 control byte 分组探测、H1/H2 hash 拆分、probe sequence、load factor 阈值等机制，优先引用 Abseil 可稳定复用的实现。
- 如果所需能力只存在于 Abseil internal API，不能直接依赖不稳定 internal 符号作为长期 ABI；应 vendor/fork 必要代码或按 SwissTable 思路改写，并保留来源和 license 说明。
- 不使用 `absl::flat_hash_map` 对象本身承载 serving realtime delta，因为它不暴露 slot CAS，占用/初始化状态，也不能在无外部锁下并发读写。

推荐数据布局：

```text
RealtimeAtomicHashMap
  -> capacity: fixed, power-of-two preferred
  -> atomic control bytes: SwissTable-style group metadata
  -> slots[]

Slot
  -> atomic<uint8_t> state: empty / reserved / occupied
  -> uint64_t primary_key
  -> atomic<RowRef*> latest_row
```

`control bytes` 用于快速跳过不匹配 group，减少 probe 次数；`state` 用于并发创建 slot 时保护 key 和 row pointer 的发布顺序。control bytes 必须用原子读写或等价的无 data race 机制实现，不能直接复用 `absl::flat_hash_map` 内部的非原子 control byte 存储。slot 在一个 realtime generation 内不 erase。删除业务 row 仍然通过普通 upsert 的 delete marker 表达。

`SourcePosition` 存在 `RowRef` 中。更新已有 key 时，writer 通过 `latest_row.compare_exchange` 发布新 `RowRef*`，并用当前 `RowRef` 的 source position 判断是否允许覆盖，避免较旧 replay 把较新 row 指针覆盖掉。

读路径：

```text
1. 根据 primary_key 计算 hash，生成 H1/H2。
2. 按 SwissTable probe sequence 扫描 control bytes。
3. 对 H2 匹配的 slot 读取 state。
4. state 为 occupied 时比较 primary_key。
5. key 相等后 acquire-load latest_row 并返回 RowRef。
6. 遇到 empty group 且 probe 终止时返回 miss。
```

读路径不持有 shard 级读锁，也不访问正在初始化的 slot。若观察到 `reserved`，说明 writer 正在创建 slot；读线程可以跳过该 slot 并继续 probe，或短暂重试当前 group，但不能读取 key 或 row pointer。

写路径：

```text
1. 在 hash map 外完成 row decode、schema 校验、row arena 写入和 payload pool 写入。
2. 根据 primary_key probe 目标 slot。
3. 命中 occupied 且 key 相同的 slot 时，读取当前 latest_row。
4. 比较新旧 RowRef 的 source position；新版本更大时，用 compare_exchange 发布 latest_row。
5. 遇到 empty slot 时 CAS state: empty -> reserved。
6. CAS 成功的 writer 先把 control byte 从 empty 写成当前 key 的 H2，使后续 reader 不会把该 probe chain 当作 miss 终止。
7. 初始化 primary_key 和 latest_row。
8. 最后 release-store state: reserved -> occupied。
9. CAS 失败的 writer 重新 probe 或重读该 slot。
```

写入必须先生成不可变 `RowRef`，再发布到 map。这样读线程一旦 acquire-load 到 `RowRef*`，即可读取完整 row version。若同一个 key 有并发或乱序 replay，写路径必须根据 `SourcePosition` 拒绝旧版本覆盖新版本。

容量策略：

- hash map 固定容量，不在 serving 热路径 rehash。
- 每个 shard 按预估增量 key 数 reserve，默认 load factor 不应超过 50%-70%。
- 接近容量阈值时触发 delta compaction，创建 `CompactGeneration`，构建新的 `CompactDeltaSnapshot` 和 fresh `RealtimeAtomicHashMap`，追平后再发布新的 `ShardState`。
- 容量耗尽时 fail closed：拒绝继续发布该 shard 的新增 delta，报警并触发 compaction/rebuild；不能在读写热路径执行阻塞式扩容。

内存与生命周期：

- `RowRef` 指向 append-only row arena 和 append-only string/list payload pools。
- 被替换的旧 row 不在写路径释放。
- 旧 realtime generation 在 compaction 或 shard cutover 后，等待 reader epoch drain 再整体释放。
- control bytes 和 slots 应按 cache line 对齐，避免热点 slot 的 `latest_row` 与频繁写入的元数据产生 false sharing。

## Row Layout 与 Value Encoding

主存储采用 row-based 布局，因为主访问模式是点查一条记录并读取全部字段。

```text
FixedSizeRowSlot
  -> presence_bitmap
  -> fixed_area
  -> ref_area
```

同一个 compiled schema version 下，所有 row 使用统一 slot size。这样 row 地址计算简单，cache 行为也更稳定：

```text
row_address = row_base + row_id * row_slot_size
```

第一版 artifact 和 owned compact snapshot 统一使用 little-endian。第一版只支持 little-endian host；loader 在非 little-endian host 上 fail closed。每个 row slot 起始地址 8-byte aligned，`row_slot_size` 向上取整到 8 字节。

scalar 字段存储在 `fixed_area` 中。string 和 list 不作为变长 payload 直接内联在 row 内，而是在 `ref_area` 中存固定宽度引用，再通过外部 pool 和 arena 解析。

- `presence_bitmap` 按 field ID 在 compiled layout 中的位置编号，1 bit 表示字段 present。bitmap 字节数向上取整后再 pad 到 8-byte boundary。
- `fixed_area` 中 scalar 字段按物理类型自然对齐，最大 8-byte alignment。`int8` 和 `bool` 占 1 byte；`int32` 占 4 bytes；`int64` 和 `uint64` 占 8 bytes。除 presence bitmap 外不做 bit-packing。
- `ref_area` 中 `string` 和 `list<T>` 使用 16-byte fixed-width ref，不把变长 payload 内联进 row slot。

```text
ValueRef16
  -> uint64_t offset
  -> uint32_t byte_length
  -> uint32_t element_count_or_flags
```

`string` 的 `byte_length` 表示字节数，第一版按 UTF-8 bytes 存储但不在索引层做 Unicode 规范化。`list<T>` 的 payload 连续存储，`byte_length` 表示 payload 字节数，`element_count_or_flags` 表示元素个数。field 的 pool/encoding 由 compiled layout 决定，不放在每个 ref 中重复存储。

string 和 list 字段使用字段级 encoding policy：

- `inline_ref`：row 内存固定宽度的小值引用。
- `dict`：row 内存 dictionary ID，真实值在 typed pool 中。
- `arena`：row 内存 variable-length arena 的 offset 和 length。
- `list_dict`：对整个 list value 做去重。
- `element_dict`：对重复 list 元素做去重，尤其适合重复 string 元素。

encoding policy 按字段配置。离线 builder 也可以基于 Parquet 统计信息选择默认策略，但显式配置优先。schema 新增和删除字段会产生新的 compiled layout，并可能改变 `row_slot_size`；旧 shard state 继续使用自己的原始 layout。

## 测试策略

核心测试：

- `Get` 从 full shard snapshot 能返回完整 row。
- `Get` 从 compact delta snapshot 能通过同一套 `ImmutableRowSnapshotView` 返回完整 row。
- `MGet` 对跨 shard keys 返回与输入 key 顺序一致的结果。
- realtime delta 本地发布后立刻可见。
- `UpdateCoordinator` 只有在 batch 内所有 Kafka upsert 本地发布成功后才 commit offset。
- `KafkaUpdateConsumer` 可以通过 fake Kafka client 或 recorded message batch 测试 poll、seek、lag 和 commit 行为。
- delta hit 返回整条 row，永远不和 full 做部分字段合并。
- compact delta 覆盖 full snapshot。
- realtime delta 覆盖 compact delta。
- 单 shard cutover 不影响其他 shard。
- 尚未 cutover 的 shard 仍服务 old full 加 live realtime updates。
- cutover 前积累的 RebuildGeneration realtime delta 在 shard 切换后可见。
- 外部 rebuild cutover 只有在相关 Kafka partitions lag 都低于阈值并追过安全 source position 后才允许执行。
- 外部 `AsyncLoad` 能按 shard 切换 full artifact、schema version 和 rebuild realtime delta。
- 外部 `AsyncLoad` 双写期间，旧 generation 忽略新增字段/删除字段变化，新 generation 按新 schema 编码。
- 内部 `Full Rebase` 能把 compact delta 合并进 full snapshot，并保留 rebase realtime delta 的最高优先级。
- 内部 `Full Rebase` 只扫描 immutable compact/full 输入，并通过 rebase realtime delta 保留切换窗口内的新 upsert。
- 外部 `AsyncLoad` 与内部 `Full Rebase` 冲突时，外部 `AsyncLoad` 优先且内部 rebase 被取消或延后。
- 同一个 generation 内，较旧的 source position 不能覆盖较新的 source position。
- delta compaction 构建的新 compact snapshot 等价于 sealed realtime delta 覆盖 old compact snapshot。
- delta compaction 双写窗口内的新 upsert 在 cutover 后仍可见。
- 业务删除 row 会作为带 delete marker 字段的普通 row 返回。
- schema 新增字段时，旧 snapshot 返回默认值，新 snapshot 返回实际值。
- schema 删除字段时，field ID 标记为 deleted 且永不复用。
- 已有 field ID 的类型变更会被拒绝。
- dictionary string/list 字段能正确解码。
- string/list pinned view 在 `Row` 生命周期内保持有效。
- mmap-backed full snapshot、rebase-owned full snapshot 和 owned compact snapshot 的 row decode 行为一致。
- artifact checksum 和 format validation fail closed。
- delta compaction 后 lookup 结果保持不变。

性能测试：

- 并发 reader 下单 key `Get` 延迟。
- 混合 shard 和同 shard key set 下的批量 `MGet` 延迟。
- 常见 schema 的 full-row decode 延迟。
- `ImmutableRowSnapshotView` 在 mmap-backed 和 owned-backed 下的 lookup/decode 延迟。
- realtime delta `Get` 延迟和 update publication 成本。
- mmap full shards、dictionary pools、realtime deltas 下的内存使用。
- per-shard load 和 cutover 时间。
- delta compaction CPU 成本。
- 内部 `Full Rebase` 的单 shard 构建时间、峰值内存和发布延迟。
