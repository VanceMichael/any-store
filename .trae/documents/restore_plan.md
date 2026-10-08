# 受控整库恢复（Online Restore）实现计划

## 一、仓库调研结论

### 1. 现有备份的物理形态
- 顶层 `DB.Backup`（[db.go:1073-1126](file:///Users/vance/project/mineProject/swe/09100801/project-01/db.go#L1073-L1126)）用与源库相同的 Options `btree.Open` 一个全新目标库，经 SQLite backup API 移植（[internal/btree/backup.go](file:///Users/vance/project/mineProject/swe/09100801/project-01/internal/btree/backup.go)）逐页拷贝并提交，再 `dropFormatSettled`，最后 `Close`。
- 关闭时 `pager.close`（[pager.go:3346-3457](file:///Users/vance/project/mineProject/swe/09100801/project-01/internal/btree/pager.go#L3346-L3457)）作为 last client 会做 PASSIVE checkpoint 并把 `-wal` truncate 到 0、unlink `-shm`。**备份成品是单一独立主文件，无 WAL/SHM 旁路文件**——正是需求中"备份所代表的单一文件代"。
- 手工把该文件覆盖到运行实例的主文件上的危险：残留 `-wal` 帧按 pgno 覆盖新主库页、`-shm` 哈希索引指向旧代；本引擎冷启动信任 `-wal`（salt+累计校验链恢复，[wal.go:1978-2176](file:///Users/vance/project/mineProject/swe/09100801/project-01/internal/btree/wal.go#L1978-L2176)），但 salt 相同（或文件被整体替换后 SHM 仍有效）时旧帧会被采纳 → 混合代损坏。

### 2. 可复用的既有协调机制
- **事务互斥**：`DB.writeMu` 串行化写事务；`DB.mu` RWMutex 包住 pager 操作，读者持 RLock；`readerSem` 限并发读者（[db.go:816-980](file:///Users/vance/project/mineProject/swe/09100801/project-01/internal/btree/db.go#L816-L980)）。`Close` 已示范"writeMu + db.mu.Lock 排空读者 + 强回滚在途写者"的收束模式（[db.go:703-749](file:///Users/vance/project/mineProject/swe/09100801/project-01/internal/btree/db.go#L703-L749)）。
- **checkpoint/WAL 重启**：`Checkpoint(TRUNCATE)` → `wal.checkpointWithMode` 回填主文件并 `doResetWAL` 截断 `-wal`、重随机 salt（[wal.go:3566-4116](file:///Users/vance/project/mineProject/swe/09100801/project-01/internal/btree/wal.go#L3566-L4116)）。
- **多进程文件锁**：BSD flock 三态（[dbfile_lock_unix.go](file:///Users/vance/project/mineProject/swe/09100801/project-01/internal/btree/dbfile_lock_unix.go)），`tryUpgradeDBLockExclusive` 可判断自己是否 last client；SHM 字节锁 + DMS 选举（[shm_mmap.go:146-212](file:///Users/vance/project/mineProject/swe/09100801/project-01/internal/btree/shm_mmap.go#L146-L212)）；文件身份 (dev,ino) 工具 `statIdentity/dbFileUnmoved`（inode_unix.go）。
- **崩溃恢复**：冷启动 `-shm` 永不被信任（DMS 当选者砍成 3 字节残桩），从 `-wal` 重建；但当前生产代码**从不 rename/替换主文件**，没有"代"的概念，需要新增。
- **句柄失效机制**：collection 长寿命句柄 + `closed/closePending` + catalog identity token + schema cookie/epoch 惰性重校验（[collection.go:1447-1500](file:///Users/vance/project/mineProject/swe/09100801/project-01/collection.go#L1447-L1500)、schema.go）；FTS 靠 5 个 namespace root、向量索引靠 roots+build 双重比对决定 RAM 对象能否沿用。恢复是任意内容替换，采用**确定性策略：显式失效全部已发句柄**（符合需求"重新校验**或**明确失效"），用户重新 OpenCollection 获得新句柄。
- **durability controller**：自动 flush 循环 + sentinel（[controller.go](file:///Users/vance/project/mineProject/swe/09100801/project-01/internal/durability/controller.go)、[sentinel.go](file:///Users/vance/project/mineProject/swe/09100801/project-01/internal/durability/sentinel/sentinel.go)）。`Stop/Start` 会动 sentinel，需要新增不动 sentinel 的 `Pause/Resume`。
- **完整性检查**：`(*btree.DB).IntegrityCheck()`（[integrity.go:561](file:///Users/vance/project/mineProject/swe/09100801/project-01/internal/btree/integrity.go#L561)）遍历全部页；读取路径会逐页执行 cksum/AEAD 校验，因此它同时覆盖结构完整性、校验和与加密可解密性。
- **故障注入惯例**：生产文件中以 nil 包变量/atomic.Pointer 存放 hook（零成本），如 `walReadFrameFaultHook`、`testCommitPrePublishHook`。

### 3. 关键约束
- btree `Open` 对已存在文件是"文件状态权威"：页大小不一致、加密/不加密状态不符都会拒绝；AEAD 用错密钥在首次读页时失败。用**与运行库相同的 Options 打开备份副本**即天然完成"格式/加密/页大小兼容性验证"。
- 观察到的既有局限（不在本次范围）：passphrase 派生密钥时 Backup 目标库使用新 salt，而页 1 头逐字拷贝带来源 salt，口令类加密备份可能无法重新打开；raw 32 字节 key 不受影响。Restore 的验证语义 = "备份必须能用当前 Options 打开"，与 Backup 契约一致，不顺带修改 Backup 格式。

## 二、总体设计

### 1. API

```go
// 顶层 anystore（db.go 的 DB 接口新增）
Restore(ctx context.Context, backupPath string) error

// btree 层
func (db *DB) Restore(ctx context.Context, backupPath string) error
```

新增错误（btree 定义、anystore 包装透传）：
`ErrRestoreUnsupported`（InMemory/VFS）、`ErrRestoreFormat`、`ErrRestorePageSize`、`ErrRestoreEncryption`、`ErrRestoreBusy`（其他进程持锁/上下文超时）、`ErrRestoreInProgress`（交换期间新事务快失败）、`ErrRestoreInTransaction`（禁止在事务 ctx 中调用）。

### 2. 磁盘代协议

- 暂存目录：`<dbpath>.restore.d/`
  - `new`：已验证的备份完整副本（独立主文件，无旁路文件）；
  - `old`：交换时被移走的上一代主文件；
  - `marker.json`：`{version:1, state:"prepared"|"committed"|"rollback", token, newID:{dev,ino}, oldID:{dev,ino}|null, pageSize}`，状态更新用临时文件+rename 原子替换。
- 铁律：**采纳任何一个代时，先删除同路径下现存的 `<dbpath>-wal` 与 `<dbpath>-wal-shm`**（它们不属于任何被选中的单文件代），杜绝旧 WAL/SHM 接入新主文件。
- fsync 点：new 拷贝完成；每次创建/rename 后 fsync 所在目录；marker 每次状态跃迁后 fsync。
- 冷启动确定性裁决（在 btree `Open` 读头/建 codec **之前**执行 `settleRestoreGenerations`）：
  - 无暂存目录 → 默认路径，行为零变化（仅多一次目录 Stat）；
  - `prepared`/`rollback` → **旧代获胜**：主文件缺失则 `old→主文件`，删除 `new` 与一切旁路文件；
  - `committed` → **新代获胜**：主文件即 `new`（已就位），删除 `old` 与一切旁路文件；
  - 裁决在主文件 flock（多进程 SH→EX 升级，忙则 busy 重试）下进行，完成并 fsync 目录后删除暂存目录，再走正常 Open。

### 3. 运行实例 Restore 阶段（锁只覆盖交换短窗口）

0. 前置：非 InMemory、非 VFS、DB 未关闭、ctx 无 ambient 事务；`restoreMu` 串行化同实例恢复。
1. **验证（不触碰现文件，不加 db 锁，实例全程可用）**：stat 备份；拒绝目录/与现主文件同一 (dev,ino)/位于暂存目录；`readInitialHeader` 验魔数与头；页大小必须等于 `db.PageSize()`；加密标记必须与 Options 相符（不符给明确错误）；文件大小必须是页大小整数倍且 ≥1 页。
2. **复制**：流式拷贝到 `.restore.d/<token>/new`，拷贝循环中周期性检查 ctx；fdatasync(new)、fsync 目录。
3. **结构/加密校验**：用 `db.Options()` 原样 `btree.Open(new)` → `IntegrityCheck()`（同时验证每页 cksum/AEAD）→ `Close`（成品回到单文件态）；确认无残留 `-wal/-shm`。
4. **静默**：置 `swapping`（新事务快失败 `ErrRestoreInProgress`）；anystore 层 Pause durability controller；取 `writeMu`，再 `db.mu.Lock()` 排空全部本进程读者；确认 pager 处于 `pagerOpen`。
5. **排他集群**：多进程模式 `tryUpgradeDBLockExclusive(p.file)`，按 busy 退避并响应 ctx，其他进程不关闭则返回 `ErrRestoreBusy`（此阶段之前一切未变，可重试）；in-process 本就独占。随后 `Checkpoint(TRUNCATE)` 把 WAL 全量折叠进主文件并截断——此后旧主文件是自包含单文件代。
6. 写 `prepared` marker + fsync（hook: marker_prepared）。
7. **摘离**：munmap；`wal.close(true)`（unlink shm、关 wal fd）；destroy writerCache；防御性 remove 残留旁路文件；**主 fd 保持打开且持 EX 锁**（hook: detach）。
8. **交换 rename**（EX 锁在身）：`主文件→old`、fsync 目录（hook: rename_old）；`new→主文件`、fsync 目录（hook: rename_new）；marker 置 `committed` + fsync（hook: marker_committed）。
9. **重开**：关闭旧主 fd；`p.open()` 在新主文件上重建 pager/WAL/SHM（旧排队 peer 随后命中身份重检而重开，见下）；重建 `onPublish` 回调、本地 counters、snapCounters、dataVersion（推进防 ABA）、销毁读者缓存池；更新 `openDBs` 注册键（旧 inode key → 新 inode key）（hook: reopen）。
   - 失败回滚：marker 置 `rollback`+fsync；删除新主文件与旁路文件；`old→主文件`+fsync；`p.open()` 重开旧代；成功则实例留在旧代、返回错误、暂存目录清理保留可重试；旧代也打不开 → 置 closing 致命错误，marker 留待下次 Open 裁决。
10. **收尾**：删除 `.restore.d`（含 old）与旁路文件、fsync 目录（hook: cleanup）；释放 `db.mu`/`writeMu`、清 `swapping`。
- 任一阶段失败或 ctx 取消：交换前仅删暂存目录；交换后按第 9 步回滚。旧代始终可重试。

### 4. 防 stale-fd peer：pager.open 身份重检
在 `pager.open` 获锁后增加：fstat(fd) 身份 vs `os.Stat(path)` 身份不一致（文件在 open 与获锁之间被替换）→ 关 fd、重新 open+加锁，有界重试（沿用 100×10ms 风格）；身份不可用平台（Windows/wasm/VFS）维持现状跳过。这样交换期间已在排队的他进程 fd（指向旧 inode）不会用新路径名下的 `-wal` 混入两代。

### 5. anystore 层恢复后复位（新文件 `restore.go`）
- 调用前 Pause durability controller；btree Restore 成功后：
- 显式失效全部已发 collection 句柄：持 `db.mu` 对每个句柄走 `close(true)` 等价路径（仅摘注册表+置 closed，**不删任何数据/namespace**），清空 `openedCollections/byIdentity`、`sketchDirty/ftsDirty`；旧句柄之后返回 `ErrCollectionClosed`，重新 `OpenCollection` 得到新句柄。
- `systemNS=nil` 后重新 `init(ctx)`；用 `LocalCounters()` 重置 epoch 为全新一代（`gen+1, start=known=cookie`，纵深防御）；`sketchEpoch+1`、清 settling/ownCookie。
- 仿正常 Open 跑 `upgradeIndexFormats(ctx)`（备份已 drop settled 记录），然后 Resume controller。
- DB 对象本身（instanceId、closed 标志、接口身份）不变 → "现有数据库句柄继续工作"；Stats/FTS/向量/再次 Backup 均在新内容上自然运行。

## 三、改动文件清单

| 文件 | 改动 |
|---|---|
| `internal/btree/restore.go`（新增） | 代协议、marker、暂存/校验/交换/回滚、`DB.Restore`、`settleRestoreGenerations`、故障 hook |
| `internal/btree/db.go` | Open 开头接冷启动裁决；DB 增加 `restoreMu/swapping`；beginRead/BeginWrite 增加 swapping 快失败；重开后 counters/hook 初始化复用（抽取小helper）；openDBs 键刷新 |
| `internal/btree/pager.go` | `p.open` 获锁后身份重检循环；新增 `detachForRestore`（不触发 checkpoint/不删主文件的摘离） |
| `internal/btree/osfuncs.go`、`osfuncs_sync_other.go`/`_linux.go` | 新增 `osRename`、`osRemoveAll`(或直接 os)、`fsyncDir` 可替换变量 |
| `internal/durability/controller.go` | 新增 `Pause()/Resume()`：停止并 join 自动 flush goroutine，不触碰 running 之外状态/sentinel |
| `restore.go`（顶层新增） | `db.Restore`：事务检查、controller pause/resume、调 btree、句柄批量失效、systemNS/epoch 复位、upgradeIndexFormats |
| `db.go`（顶层） | DB 接口增加 `Restore` 方法声明 |
| `errors.go` | 增加 `ErrRestore*` 错误变量 |
| `internal/btree/restore_test.go`（新增） | 见验证矩阵 |
| `restore_test.go`（顶层新增） | 见验证矩阵 |

不改动：backup.go 与备份格式、WAL 帧格式、加密格式、默认 Open 行为（无 marker 时仅多一次目录 stat）。

## 四、实现步骤（依赖顺序）

1. osfuncs：`osRename`/`fsyncDir` 封装（含非 linux 回退）。
2. btree：marker 与冷启动裁决 `settleRestoreGenerations` + pager.open 身份重检；接入 `Open`。
3. btree：`pager.detachForRestore` 与重开复用（p.open 现状即可直接重开，重点是摘离顺序）。
4. btree：`DB.Restore` 全流程（验证→复制→校验→静默→TRUNCATE→marker→摘离→rename→重开→清理/回滚）+ hook 与 ctx 取消点。
5. durability：`Pause/Resume`。
6. anystore：接口、错误、顶层 `db.Restore` 与句柄/epoch/systemNS 复位。
7. 测试（与实现交替，先 btree 后顶层）。

## 五、验证方案

`go build ./... && go test ./internal/btree ./... -run Restore -count=1`，再跑全量 `go test ./...`（按 Makefile 既有目标）。

btree 层新增测试：
1. 基本恢复：旧内容写入→Backup→追加写入→Restore→同一 *DB 句柄读到备份内容；再次写入、checkpoint、关闭重开后内容稳定。
2. 验证拒绝矩阵：垃圾文件、魔数损坏、页大小不符、文件大小非整页、加密⇄明文错配、错误密钥（raw key）；每种失败后现库完好可写。
3. 明文/cksum/AEAD(raw key) 三种完整性模式下备份→恢复往返。
4. 每个故障 hook（copy/marker/detach/rename_old/rename_new/marker_committed/reopen/cleanup）注入失败 + ctx 取消：暂存目录清理、旧代可用且可重试、无旁路文件残留；失败后再写数据并 TRUNCATE+重开，证明旧 WAL 帧没有覆盖新内容。
5. 崩溃边界：hook 令交换后跳过回滚并关闭句柄（模拟进程死亡），分别在 prepared/committed 边界用**新的 btree.Open** 验证确定性选中旧/新代，内容完整、无 `-wal/-shm` 串代。
6. 多进程（linux/darwin build tag，仿 multiprocess_test.go 子进程模式）：他进程持开时 Restore 返回 busy；他进程退出后恢复成功；交换窗口内并发 Open 只见到前/后完整状态（身份重检）。
7. in-process（heap shm）模式同样往返；InMemory 返回 ErrRestoreUnsupported。

anystore 层新增测试：
8. 含 FTS 索引、向量索引、普通二级索引、自定义主键/压缩的库：Backup→大量变更（drop/recreate 同名集合）→Restore；验证 Find/Stats/$text/KNN 全部基于恢复内容；恢复后再次 Backup→Restore 幂等。
9. 旧 Collection 句柄恢复后返回 `ErrCollectionClosed`，重新 OpenCollection 正常；DB 句柄不换。
10. durability AutoFlush 开启时恢复不与后台 flush 竞争；恢复后写事务正常 arm sentinel。
11. ambient 事务 ctx 中调用 Restore 被拒；恢复中发起新事务得 ErrRestoreInProgress。

## 六、风险与处理

- **pager.open 身份重检影响开库热路径**：仅一次额外 fstat+stat，且身份不可用平台跳过；有界重试，耗尽返回明确错误。
- **他进程长期持有 SHARED flock**：恢复无法强制其关闭（flock 与打开生命周期绑定），按 busy handler + ctx 等待，超时 `ErrRestoreBusy`，且在此之前不修改任何文件 → 无副作用、可重试。文档与错误信息说明"恢复需要其他进程先关闭库"。
- **交换后排队 peer 的 stale fd**：由身份重检兜底（关 fd 重开路径），SHM DMS 选举保证不会加入他代索引。
- **TRUNCATE checkpoint 失败**：发生在任何文件改动之前，直接失败、可重试。
- **回滚后旧代重开也失败（磁盘级灾难）**：标记 handle closing，保留 marker 交下次 Open 确定性裁决，不静默吞错。
- **VFS/wasm 平台**：无 rename/fsyncDir 原语，Restore 直接返回 ErrRestoreUnsupported；冷启动裁决代码仅依赖现有 os* 可替换变量。
- **口令类加密备份的既有 salt 局限**：验证阶段会以"当前 Options 打不开备份"拒绝，属正确性拒绝；不改变现有备份格式与默认行为。
