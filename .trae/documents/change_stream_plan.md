# Collection Change Stream（带 Resume Token）实施计划

## 一、需求要点（验收基线）

1. 面向 collection identity 的变更流入口；每个成功提交的 insert/update/delete 按事务内确定顺序产生带单调位置（sequence）的事件。
2. 事件与文档及 range / full-text / vector 索引在**同一提交**中可见；事务回滚、失败提交、被 savepoint 回滚的操作不产生事件。
3. 可保存的 resume token：从 token 重连时重放其后事件，不重复 token 所代表事件；多订阅者、同进程多读者互不干扰。
4. 另一进程提交后，现有订阅通过数据库**已有**的跨进程变更可见机制（WAL 帧 + mmap WAL-index 头）收到事件。
5. collection rename 沿用原 identity；drop 后旧流明确结束；同名新建 collection 不能接续旧序列。
6. ctx 取消 / 文件错误 / 积压：保留最后一个已确认 token、释放读事务、可从该 token 重试；旧事件可安全清理，但不能删除未确认或仍在重放范围内的记录。
7. 无订阅者时 Open、普通事务、查询、聚合、Backup、关闭与文件格式保持兼容。

## 二、仓库研究结论

- 顶层包 `anystore`（module `github.com/anyproto/any-store/v2`），存储引擎 `internal/btree`：页级 B-tree + WAL；单写者（`DB.writeMu`）多读者；darwin/linux + amd64/arm64 使用 mmap SHM（[shm_mmap.go](file:///Users/vance/project/mineProject/swe/09100801/project-03/internal/btree/shm_mmap.go)），其余平台堆 SHM。
- Collection 编目位于 `_system` 命名空间：`coll:<name>` 的值为 8 字节随机 catalog token（[newCatalogID](file:///Users/vance/project/mineProject/swe/09100801/project-03/db.go#L351-L355)），旧文件为共享值 `"1"`。数据命名空间名 = collection 名。句柄 `collection.identity` = catalog token + 4 字节数据根页（[identify](file:///Users/vance/project/mineProject/swe/09100801/project-03/schema.go#L818-L833)），注册表同时按名字和 identity 索引（`openedCollections` / `byIdentity`）。
- rename：[renameCollection](file:///Users/vance/project/mineProject/swe/09100801/project-03/db.go#L1846-L2005) 在同一事务内原样移动 `coll:` 记录（token 不变）并重命名所有派生命名空间。drop：[Collection.Drop](file:///Users/vance/project/mineProject/swe/09100801/project-03/collection.go#L1380-L1437) 在同一事务删除 catalog、数据及所有派生命名空间，提交点（`txSchema.install` / `settleLog`）关闭句柄。
- 提交路径：[writeTx.Commit](file:///Users/vance/project/mineProject/swe/09100801/project-03/tx.go#L232-L302) 顺序为 `flushAllFtsPending` → `persistAllDirtySketches` → `advanceFormatSettled` → 通告 cookie → btree `Commit`；`OnCommitted` 回调在"变为可见"的瞬间执行。回滚/失败提交/savepoint 回滚会丢弃 FTS pending 与 schema log（[savepointWrapper.Rollback](file:///Users/vance/project/mineProject/swe/09100801/project-03/tx.go#L390-L417)，`logMark` 截断）。
- 跨进程可见性：提交写入 WAL 帧并更新 mmap WAL-index 头；新的 `BeginRead` 经 `beginReadHdr` 读到最新已提交头（[beginRead](file:///Users/vance/project/mineProject/swe/09100801/project-03/internal/btree/db.go#L816-L980)）。**无推送**，`checkStale` 在事务开始时观察，故 change stream 沿用"短读事务 + 轮询 WAL-index"，同进程另加低延迟唤醒。
- Backup 为页拷贝（[Backup](file:///Users/vance/project/mineProject/swe/09100801/project-03/db.go#L1073-L1126)），所有命名空间自动包含；新增命名空间无需文件格式版本变更（与 fts/vector 命名空间加入方式一致）。
- 压缩文档以 `TypeCompressedObjectS2` 首字节自描述，anyenc Parser 透明解码，事件内可直接保存"数据命名空间实际写入的字节"。

## 三、总体设计

### 3.1 事件存储：按 catalog token 派生的独立命名空间

- 每个 collection 一个变更流命名空间，名字为 `csx:<hex(catalog token)>`。
  - 名字**只**依赖不可变的 catalog token：rename 时无需任何重命名；drop 时随数据命名空间一起删除；同名重建的 collection 有新 token → 新命名空间，物理上无法接续旧序列。
  - 旧文件共享 token `"1"`：派生名追加数据根页消歧（`csx:31_<rootHex>`），根页 rename 不变；其 drop+立即重建复用根页的极端情形记为已知限制（仅限 token 机制存在之前创建的文件）。
- 事件 key = 8 字节大端 sequence（从 1 开始）；value 用 anyenc 编码：
  - `op`：1=insert / 2=update / 3=delete
  - `id`：主键的原始编码字节
  - `d`：实际写入数据命名空间的文档字节（insert/update；可能为 S2 压缩自描述格式）
- 元数据记录 key=`[]byte{0}`（恒排在 8 字节事件 key 之前）：`{first, last}`——first=当前保留的最小序号（清理锚点），last=最新序号。

### 3.2 事务内采集与提交（事件与数据/索引同一原子提交）

- `commonTx` 新增 writer 拥有的 `changeBuffer`（挂在 [commonTx](file:///Users/vance/project/mineProject/swe/09100801/project-03/tx.go#L63-L89)），保存 `[]pendingChange{op, *collection, id 字节副本, doc 字节副本}`。
- 挂钩点（[collection.go](file:///Users/vance/project/mineProject/swe/09100801/project-03/collection.go)）：
  - `insertItem`：全部索引写入成功后追加 insert 事件（按多文档循环顺序）。
  - `update`：仅在实际修改（modified=true）时追加 update 事件；内容未变不产生事件。
  - `deleteItem`：在数据记录删除处追加 delete 事件。
  - 索引回填路径（`buildIndex`/`buildFtsIndex` 直接走 `idx.insertKeys`）不经过这些函数，天然不产生文档事件。
- Savepoint：`savepointTx` 增加 `changeMark = len(buffer)`（与 `logMark` 同处，[reset](file:///Users/vance/project/mineProject/swe/09100801/project-03/tx.go#L356-L361)）；RollbackTo 截断 mark 之后的待写事件；Release 保留。嵌套 savepoint 由外层 mark 覆盖。
- 提交：在 [writeTx.Commit](file:///Users/vance/project/mineProject/swe/09100801/project-03/tx.go#L232-L302) 的 `persistAllDirtySketles` 之后、btree Commit 之前新增 `prepareChangeEvents`：
  1. 按 collection 分组，惰性创建 `csx:` 命名空间（首次有写入时）；
  2. 从 meta 读 last，按 buffer 顺序分配连续 sequence；
  3. Put 事件记录并重写 meta{first,last}——全部在当前 btree WriteTx 内，随数据与三类索引同一提交可见；
  4. 任何失败 → 走现有 unwind（丢弃 buffer + btree 回滚），不产生事件。
- 全量回滚/失败提交：buffer 随事务丢弃（与 FTS pending 同处置）；新写事务开始时 reset。

### 3.3 Resume Token

- `type ResumeToken []byte`，不透明编码：版本字节(1) + catalog token（长度前缀 + 原始字节）+ 8 字节 sequence。提供 `Marshal`/`Unmarshal` 方法。
- nil token：按 `WatchOptions.StartFrom` 决定从头（Beginning，默认）还是从当前最新（Now）。
- 重连校验（在被订阅 collection 的视图中）：
  - token catalog token ≠ 当前 collection → `ErrInvalidResumeToken`（同名重建/张冠李戴时明确报错）；
  - seq ≥ meta.first：定位事件存在性，从其后读；seq == first-1（最后被清理事件）仍合法，从 first 重放；seq < first-1 → `ErrChangeHistoryLost`；seq > last → 尾部等待。
  - token 代表的事件本身不重放（seek key = seq+1）。

### 3.4 订阅入口与读取循环

- 入口（identity 导向）：`Collection.Watch(ctx, opts...) (ChangeStream, error)`；句柄随 rename 继续工作；另有 `DB.WatchCollection(ctx, name, opts...)` 便捷方法（经注册表解析到同一句柄）。
- API 风格与现有迭代器一致：
  ```go
  type ChangeStream interface {
      Next(ctx context.Context) (ChangeEvent, error)
      Close() error
  }
  type ChangeEvent struct {
      Operation OperationType
      Key       anyenc.Tuple       // 主键原始编码
      Doc       *anyenc.Value      // insert/update 的完整文档（自描述字节解析）
      ResumeToken ResumeToken
  }
  ```
- 每次轮询：开始**新的短读事务**（`db.ReadTx`，含 checkStale）→ 解析 collection（先用当前句柄 `resolve`；失败时按 catalog token 全目录扫描 identity，实现对端 rename 后无感切换到新句柄；找不到则判定 drop）→ cursor seek 到 token.seq+1 → 读取至多 BatchSize 条到内存批 → **立即释放读事务** → 再逐条交付。
  - 积压：读取永远不超过一个批次、交付时不持有读事务；消费慢不会长期占压读者槽与旧快照；token 只在 `Next` 返回时推进。
  - ctx 取消/文件错误：结束本轮并回滚读事务，返回错误但不推进 token；下次 `Next` 从原 token 重试。
  - 追平后等待：同进程提交经新增的进程内 `changeNotifier`（sync.Cond，仅在有等待者时由提交后唤醒）即时唤醒；跨进程按 `Config.ChangeStreamPollInterval`（默认 200ms）轮询 WAL-index——即数据库已有跨进程可见机制。
- 生命周期终态：
  - drop：内存批交付完后，下轮轮询发现 identity 消失且 `csx:` 命名空间已删除 → `ErrChangeStreamEnded`（明确结束）。
  - 同名新建：经新名字拿到新句柄，旧 token 校验 → `ErrInvalidResumeToken`；新流 sequence 从 1 开始。
  - `Close()`：幂等，唤醒等待中的 Next → `ErrChangeStreamClosed`。

### 3.5 旧事件清理

- `Collection.PurgeChangeEvents(ctx, token ResumeToken) error`：删除 seq ≤ token.seq 的事件记录，推进 meta.first = token.seq+1；**只删 token 之前（含）**，未确认（其后）记录不动。
- token 本身成为新锚点：以该 token resume 仍合法（first-1 规则）；更早 token → `ErrChangeHistoryLost`。由调用方保证所有持久订阅者都已确认该位置（实现无法知道外部订阅者，故不做自动清理）。

### 3.6 兼容性

- 无文件格式/魔数变更；旧文件直接打开，其流从升级后首次写入开始（升级前写入无事件，文档说明）。
- `csx:` 命名空间对 Open 透明；Backup 页拷贝自动包含；Close 时随库关闭；不新增普通事务的锁语义（仅在已有提交流程中增加同事务写入）。
- InMemory 模式同样适用。

## 四、改动文件

- 新增 `change_stream.go`：ResumeToken、WatchOptions、ChangeEvent、ChangeStream、OperationType、哨兵错误（ErrChangeStreamEnded / ErrChangeStreamClosed / ErrInvalidResumeToken / ErrChangeHistoryLost）、changeBuffer/pendingChange、changeNotifier、`csx:` 命名空间名派生、prepareChangeEvents、轮询与清理实现。
- 修改 [collection.go](file:///Users/vance/project/mineProject/swe/09100801/project-03/collection.go)：Collection 接口增加 `Watch`、`PurgeChangeEvents`；在 insertItem/update/deleteItem 挂钩 buffer；Drop 中删除 `csx:` 命名空间。
- 修改 [tx.go](file:///Users/vance/project/mineProject/swe/09100801/project-03/tx.go)：commonTx 增加 changeBuffer；WriteTx 接口增加 buffer 访问方法；writeTx.Commit 调用 prepareChangeEvents 并在成功后唤醒 notifier；savepointTx 增加 changeMark 及回滚截断。
- 修改 [db.go](file:///Users/vance/project/mineProject/swe/09100801/project-03/db.go)：db 结构增加 changeNotifier（Open 中初始化）；newWriteTx 重置 buffer；新增 identity 目录扫描辅助。
- 修改 [config.go](file:///Users/vance/project/mineProject/swe/09100801/project-03/config.go)：`ChangeStreamPollInterval time.Duration`（默认 200ms）。
- 新增 `change_stream_test.go`：覆盖下列验证项；跨进程用例复用 `os.Args[0]` 子进程模式（构建标签 `(linux || darwin) && (amd64 || arm64)`）。

## 五、实施步骤（依赖顺序）

1. change_stream.go：类型定义、token 编解码、命名空间名派生、changeBuffer/changeNotifier 基础设施与哨兵错误。
2. tx.go / db.go：buffer 字段、重置与访问接口、savepoint mark 与截断。
3. collection.go：三个写挂钩；Drop 删除流命名空间。
4. writeTx.Commit：prepareChangeEvents（惰性建 ns、分配序号、写事件与 meta）+ 提交后唤醒；失败走 unwind。
5. Watch/Next 轮询循环（短读事务、identity 重定位、批次、等待唤醒/超时/ctx）与 Close。
6. PurgeChangeEvents。
7. change_stream_test.go：全部验证用例。

## 六、验证

- `go build ./...`、`go vet ./...`、`go test ./...`（含现有全部用例不回归）。
- 针对性测试：
  1. 插入/更新/删除事件顺序与单调序号；事件文档与数据命名空间一致、同提交可见（读快照中同时出现）。
  2. 保存 token → 关闭流并重启进程 → 重放无重复无遗漏（token 事件不重放）。
  3. 回滚、提交失败、savepoint 回滚 → 无事件；RELEASE savepoint → 事件保留。
  4. 多订阅者各自独立计数；同进程多读者并发不串扰。
  5. 子进程提交后，本进程现存 Next 被唤醒并收到事件（默认轮询间隔内）。
  6. 同进程 rename：流不中断、旧 token 继续有效；对端 rename：identity 重定位后无感继续。
  7. drop → ErrChangeStreamEnded；同名新建 → 旧 token ErrInvalidResumeToken、新流从 1 开始。
  8. Next 等待中取消 ctx：返回 ctx.Err()，token 不变，再次 Next 成功；模拟文件错误后重试成功。
  9. Purge：token 及之前事件删除、meta.first 推进；token 仍可 resume；更早 token → ErrChangeHistoryLost；其后记录均在。
  10. 带事件库执行 Backup，备份可读且事件序列一致；无订阅时 CRUD/聚合/关闭行为不变。

## 七、风险与处理

- **对端 rename 后旧句柄 retire**：轮询按 catalog token 扫描目录重定位到新句柄，token 全流程有效；同进程 rename 句柄本就不中断。
- **慢消费者与读事务长期占用**：交付前释放读事务、批次有界；追平后完全无读事务。
- **旧文件共享 token `"1"` 冲突**：追加根页消歧；仅旧文件且 drop+立即重建复用同一根页的理论情形为已知限制（有文档说明）。
- **WAL checkpoint 使慢订阅者历史消失**：以 ErrChangeHistoryLost / ErrChangeStreamEnded 明确暴露，不静默跳号。
- **prepareChangeEvents 失败**：复用 unwind 路径，buffer 与 btree 写入一并回滚，保证"无事件即无数据"。
