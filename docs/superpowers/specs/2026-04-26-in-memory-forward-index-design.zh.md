# 纯内存正排索引设计

## 目标

构建一个嵌入式 C++20 库，提供低延迟的纯内存正排查询：

```text
uint64_t primary_key -> structured value
```

组件面向高并发、读多写少的在线查询场景优化。Kafka upsert 被目标 shard 的 active generation consumer 本地发布成功后，后续查询必须立刻可见。读路径必须读取一个完整 row version，不能看到部分写入的 value。

value schema 按 schema version 固定，但 schema 本身支持运行期热加载。schema 演进只支持新增字段和删除字段；已有字段类型不能变更，field ID 永不复用。

## 非目标

- SDK 不负责把全量 artifact 投递到在线机器。
- SDK 不暴露业务手动调用的 publish API。
- SDK 不提供索引层 delete 操作。删除由普通 upsert 字段表达，例如 `is_deleted`。
- 第一版不提供跨所有 shard 的全局多 key 快照隔离。核心语义是单 key 查询一致性。
- 第一版不针对字段扫描场景优化。核心访问模式是点查一条记录，并读取全部字段。

## 在线 API 草图

```cpp
class ForwardIndex {
 public:
  std::optional<Row> Get(uint64_t primary_key) const;
  std::vector<std::optional<Row>> MGet(absl::Span<const uint64_t> primary_keys) const;

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

`ShardState` 是内部类型；public query API 不暴露 `CurrentShard`。`Row` 必须持有 pinned shard state，保证 row arena 和 dictionary pool 中的引用在 row 生命周期内始终有效。`MGet` 的返回结果与输入 keys 保持相同顺序，内部按 shard 对 key 分组，避免重复 acquire 同一个 shard pointer。

## 一致性模型

索引提供单 key 快照语义：

- 一次 lookup 只观察一个 shard state。
- 在该 shard 内，lookup 看到一个完整 row version。
- lookup 永远不会看到部分编码完成的 row。
- 全量更新期间，不同 shard 可以在不同时间切换到新的 full artifact。

设计刻意不保证跨 shard 的多 key 请求观察同一个全局 full version。更强的保证需要全局 manifest，也会增加全量更新时的内存压力。

## 核心架构

索引按 primary key 分 shard。每个 shard 可以独立加载、压实和切换。

```text
ForwardIndex
  -> ShardDirectory
       -> std::atomic<std::shared_ptr<const ShardState>>[N]

ShardState published container
  -> FullSnapshotView
       -> ImmutableRowSnapshotView mmap-backed
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

每条 Kafka upsert 都是完整 row。命中 delta 后即可返回完整 value，所以读路径永远不需要把 base 和 delta 的字段做合并。

`FullSnapshotView` 和 `CompactDeltaSnapshot` 都是薄 wrapper，读侧都委托给 `ImmutableRowSnapshotView`。两者使用相同的 primary-key lookup、row decode、string/list pool 解析和 schema/layout 校验逻辑；差异只在 backing memory 和附加 metadata。

## 运行期 Schema

schema 是运行期对象，并包含在每个全量 artifact 中。每个 shard state 绑定一个 schema version 和 compiled row layout。

字段使用稳定的 `FieldId` 标识，而不仅仅依赖字段名：

```text
FieldId -> name, type, repeated/list flag, nullable/default, encoding, status
```

schema 演进规则：

- 新增字段会创建新的 field ID。
- 删除字段会把字段标记为 tombstoned。
- field ID 永不复用。
- 字段类型变更会被拒绝。
- row 只能用它所属 schema version 的 compiled layout 解释。

客户端应在热路径外把字段名解析成 `FieldId` 和 `FieldAccessor<T>`。accessor 携带 schema-version 校验；当它被用于不兼容的 shard state 时 fail fast。

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
1. 解码并校验 Kafka row。
2. 把完整 row 编码为一个不可变 RowRef：fixed row slot 写入 row arena，
   string/list payload 写入该 realtime generation 的 append-only payload pools，
   row slot 内只保存固定宽度引用。
3. 在该 shard 的 realtime table 中查找 primary-key slot；slot 不存在时，通过 CAS 创建。
4. release-store 新 RowRef* 到该 slot。
5. 本地发布成功后再 commit Kafka offset。
```

第 4 步完成后，后续读请求即可看到新 row。旧 row memory 不在写入路径上立即回收；被替换的 row 会保留在 append-only arena 中，直到 realtime table generation 完成 compaction，并且 reader epoch 证明没有查询还可能引用旧 row 后再释放。

realtime table 避免读路径应用层锁。slot 创建使用 CAS 更新 slot key state；读路径 common path 是 open-addressing probe 加 atomic pointer load。hash table 的具体设计见 `RealtimeAtomicHashMap`。

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
  -> 把同样 row format 写入 RebuildGeneration shards
  -> shard 切换到 RebuildGeneration 后，作为该 shard 的 authoritative visible stream
```

这两条流实现成两个独立 Kafka consumer。rebuild 期间，两个 consumer 都保持全局运行。Live Apply 持续写 ActiveGeneration 的所有 shard，直到整个 rebuild 完成。Rebuild Catch-up 从 watermark `W` 开始持续写 RebuildGeneration 的所有 shard，直到所有 shard 完成切换。某个 shard 切换后，Rebuild Catch-up 成为该 shard 的 active generation consumer；Live Apply 写入旧 ActiveGeneration shard 的数据不再 serving-visible。

单 shard 切换流程：

```text
1. 从 sharded artifact 加载 shard_i 的 new_full。
2. 确认 RebuildGeneration shard_i 已追过 Live Apply 为 shard_i 发布过的最新 source position。
3. 把 new_full_i 绑定到 RebuildGeneration shard_i。
4. 原子替换 active_shards[i] 为 RebuildGeneration shard_i。
5. 两个 Kafka consumer 继续保持全局运行。
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

shard 切换后，该 shard 的查询读取 RebuildGeneration。Live Apply 继续全局消费并写入 ActiveGeneration，直到整个 rebuild 完成，但这些写入对已切换 shard 不再 serving-visible。Rebuild Catch-up 是已切换 shard 的 serving-visible stream，并且必须按照配置的 lag threshold 保持追平。

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
RealtimeDeltaAtomicTable
  -> scan latest RowRef per key
  -> SnapshotBuilder 构建 owned ImmutableRowSnapshot
  -> 作为 CompactDeltaSnapshot 发布
  -> create a fresh empty RealtimeDeltaAtomicTable
  -> atomically publish new ShardState
```

`CompactDeltaSnapshot` 按 primary key 存储自 full snapshot 以来发生变更记录的完整更新后 row slot。它与 `FullSnapshotView` 使用同一个 `ImmutableRowSnapshotView` 读接口；`FullSnapshotView` 的 backing 来自 mmap artifact，`CompactDeltaSnapshot` 的 backing 来自 compaction 生成的 owned heap snapshot。

读路径层数保持固定：

```text
realtime_delta -> compact_delta -> full_snapshot
```

如果某个 shard 的 compact delta 超过配置阈值，组件报警并在下一次 full rebuild 中优先处理该 shard。

## 并发模型

- 查询线程读取一个不可变 shard state 及其读优化 realtime delta。
- realtime delta 使用 CAS slot creation 和 atomic row pointer publication 保证即时可见。
- full rebuild 每次只加载和切换一个 shard。
- 只要还有查询持有 shared reference，旧 shard state 就会继续存活。
- 同一时刻应该只允许一个 active full rebuild generation。
- per-shard cutover 和 rebuild catch-up 必须由一个 update coordinator 协调。

## 内存模型

全量更新峰值内存由 shard batch size 控制，而不是由完整数据集大小控制：

```text
active full dataset
+ currently loaded new full shard batch
+ active realtime deltas
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

## 主键索引

full snapshot 和 compact delta snapshot 在 serving 读路径中都使用 frozen primary-key index view：

```text
FrozenPrimaryKeyIndexView: primary_key -> row_offset
```

full snapshot 的 index 从 mmap artifact 读取；compact delta snapshot 的 index 由 compaction 生成并由 heap backing 持有。两者对读路径暴露相同的 lookup API。`absl::flat_hash_map<uint64_t, RowOffset>` 仍适合后台构建阶段的临时索引，但 serving-visible 的 compact snapshot 应在 seal 后转换成 `FrozenPrimaryKeyIndexView`，以复用 full snapshot 的读路径。serving-visible 的 realtime delta 使用下面定义的 `RealtimeAtomicHashMap`。

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

`FullSnapshotView` 是 `ImmutableRowSnapshotView + artifact metadata/source watermark` 的薄 wrapper。`CompactDeltaSnapshot` 是 `ImmutableRowSnapshotView + compact generation metadata/build stats` 的薄 wrapper。这样 full 和 compact 的二进制 encoding 可以保持一致，lookup、row decode、schema evolution 和 dictionary/list 解析测试也可以共用同一套用例。

`SnapshotBuilder` 可以在构建阶段使用 `absl::flat_hash_map`、vector sort 或其他临时结构收集 key 到 row offset 的映射；一旦 seal 成 `OwnedSnapshotBacking`，serving 读路径只看到 frozen index view。

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
- 接近容量阈值时触发 delta compaction，构建新的 `CompactDeltaSnapshot` 和空的 `RealtimeAtomicHashMap`，再发布新的 `ShardState`。
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

scalar 字段存储在 `fixed_area` 中。string 和 list 不作为变长 payload 直接内联在 row 内，而是在 `ref_area` 中存固定宽度引用，再通过外部 pool 和 arena 解析。

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
- delta hit 返回整条 row，永远不和 full 做部分字段合并。
- compact delta 覆盖 full snapshot。
- realtime delta 覆盖 compact delta。
- 单 shard cutover 不影响其他 shard。
- 尚未 cutover 的 shard 仍服务 old full 加 live realtime updates。
- cutover 前积累的 RebuildGeneration realtime delta 在 shard 切换后可见。
- 同一个 generation 内，较旧的 source position 不能覆盖较新的 source position。
- 业务删除 row 会作为带 delete marker 字段的普通 row 返回。
- schema 新增字段时，旧 snapshot 返回默认值，新 snapshot 返回实际值。
- schema 删除字段时，field ID tombstone 且永不复用。
- 已有 field ID 的类型变更会被拒绝。
- dictionary string/list 字段能正确解码。
- mmap-backed full snapshot 和 owned compact snapshot 的 row decode 行为一致。
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

## 待决问题

- `FixedSizeRowSlot` 的精确二进制编码、对齐方式和字节序。
- 第一版支持的 scalar 类型和 list element 类型集合。
- shard count 和 shard assignment function。
- `FrozenPrimaryKeyIndexView` 的 mmap-backed 与 owned-backed 二进制布局。
- realtime delta compaction、rebuild delta limit、shard cutover batch size 的默认阈值。
- field accessor 在不兼容 schema version 下 fail fast。
- 第一版 full artifact 强制使用 mmap。
