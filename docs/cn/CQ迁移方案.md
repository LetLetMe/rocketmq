# File CQ 到 KV CQ 迁移方案

## 背景

RocketMQ支持两种ConsumeQueue实现：
- **File CQ (SimpleCQ)**: 基于文件系统的传统实现，通过`ConsumeQueue`类实现
- **KV CQ (RocksDBCQ)**: 基于RocksDB的实现，通过`RocksDBConsumeQueue`类实现

`CombineConsumeQueueStore`支持双写机制，可以同时维护两种CQ类型，这为平滑迁移提供了基础。

## 方案五：按Offset路由方案（新增，推荐用于平滑迁移）

### 方案概述
新消息只写入KV CQ，读取时根据offset自动路由：旧offset从File CQ读取，新offset从KV CQ读取。这是最平滑的迁移方案，无需双写，无需历史数据迁移。

### 实施步骤

1. **配置修改**：
   ```properties
   # 启用双写模式（需要同时加载File CQ和KV CQ）
   rocksdbCQDoubleWriteEnable=true
   combineCQLoadingCQTypes=default;defaultRocksDB
   
   # 启用按offset路由模式：新消息只写KV CQ
   combineCQWriteOnlyRocksDB=true
   
   # 配置灰度topic列表（支持通配符，如：test*,prod-*）
   # 如果为空，则所有topic都会启用只写KV CQ模式
   combineCQWriteOnlyRocksDBTopics=test*,prod-*
   
   # 读取优先使用KV CQ（但实际会根据offset自动路由）
   combineCQPreferCQType=defaultRocksDB
   
   # offset分配使用KV CQ
   combineAssignOffsetCQType=defaultRocksDB
   ```

2. **重启Broker**：
   - Broker启动后会同时加载File CQ和KV CQ
   - 新消息只会写入KV CQ
   - 读取时会根据offset自动路由：
     - offset < File CQ的maxOffset：从File CQ读取
     - offset >= File CQ的maxOffset：从KV CQ读取

3. **验证**：
   - 验证新消息正常写入KV CQ
   - 验证旧消息仍能从File CQ读取
   - 验证新消息能从KV CQ读取

4. **等待File CQ数据自然过期**（可选）：
   - 如果数据有TTL，等待File CQ数据过期后，可以停止加载File CQ
   - 配置修改：
     ```properties
     combineCQLoadingCQTypes=defaultRocksDB  # 只加载KV CQ
     combineCQWriteOnlyRocksDB=false  # 不再需要路由
     ```

### 工作原理

1. **写入逻辑**：
   - 当`combineCQWriteOnlyRocksDB=true`时，`putMessagePositionInfoWrapper`只写入KV CQ
   - File CQ保持不变，只包含历史数据

2. **读取路由逻辑**：
   - `findConsumeQueue(topic, queueId, offset)`方法根据offset选择store：
     - 获取File CQ的maxOffset作为切换点
     - 如果`offset < File CQ的maxOffset`：返回File CQ
     - 如果`offset >= File CQ的maxOffset`：返回KV CQ

3. **Offset范围**：
   - `getMinOffsetInQueue`: 返回File CQ的minOffset（如果File CQ存在）
   - `getMaxOffset`: 返回KV CQ的maxOffset（因为新消息在KV CQ）

### 优点
- **最平滑**：无需双写，无需历史数据迁移
- **自动路由**：根据offset自动选择正确的CQ，对用户透明
- **零停机**：可以随时启用，无需停机
- **存储效率**：新消息只写一份，不占用双倍存储
- **向后兼容**：旧数据仍可正常读取

### 缺点
- 需要同时加载File CQ和KV CQ（占用内存）
- File CQ数据需要等待自然过期或手动清理

### 适用场景
- **生产环境平滑迁移**：最适合生产环境，风险最低
- **有历史数据的场景**：需要保留旧数据读取能力
- **数据有TTL的场景**：可以等待File CQ数据自然过期

### 观测工具

使用`checkCQOffsetRoute`命令可以查看CQ offset路由状态：

```bash
# 查看所有topic的路由信息
mqadmin checkCQOffsetRoute -n localhost:9876 -b 127.0.0.1:10911

# 查看指定topic的路由信息（包含queue详情）
mqadmin checkCQOffsetRoute -n localhost:9876 -b 127.0.0.1:10911 -t test-topic

# 查看集群中所有broker的路由信息
mqadmin checkCQOffsetRoute -n localhost:9876 -c DefaultCluster
```

**输出示例**：
```
Topic: test-topic
  In Grayscale List: true
  File CQ Offset Range: [0, 1000)
  KV CQ Offset Range: [1000, 2000)
  Route Cutoff Offset: 1000
  Queue Details:
    Queue 0:
      File CQ: [0, 1000)
      KV CQ: [1000, 2000)
      Route Cutoff: 1000
```

### Topic灰度配置说明

- **通配符支持**：
  - `*`: 匹配任意字符序列（如：`test*`匹配`test`, `test1`, `test-topic`等）
  - `?`: 匹配单个字符（如：`test?`匹配`test1`, `test2`等）
- **配置为空**：如果`combineCQWriteOnlyRocksDBTopics`为空，所有topic都会启用只写KV CQ模式
- **配置示例**：`"test*,prod-*,important-topic"`

## 关键配置项

- `rocksdbCQDoubleWriteEnable`: 是否启用双写（启用后使用CombineConsumeQueueStore）
- `combineCQLoadingCQTypes`: 加载哪些CQ类型，格式：`default;defaultRocksDB` 或 `default` 或 `defaultRocksDB`
- `combineCQPreferCQType`: 优先读取的CQ类型，可选值：`default` 或 `defaultRocksDB`
- `combineAssignOffsetCQType`: 分配offset使用的CQ类型，可选值：`default` 或 `defaultRocksDB`
- `combineCQWriteOnlyRocksDB`: **新增**，是否只写KV CQ（启用后新消息只写入KV CQ，读取时根据offset自动路由）
- `combineCQWriteOnlyRocksDBTopics`: **新增**，topic灰度列表，逗号分隔，支持通配符（`*`和`?`）。如果为空，所有topic都会启用只写KV CQ模式

## 方案一：双写+逐步切换方案（推荐）

### 方案概述
利用CombineConsumeQueueStore的双写能力，先启用双写，然后逐步切换读取和分配offset的CQ类型。

### 实施步骤

#### 阶段1：启用双写（新消息同时写入File CQ和KV CQ）
1. **配置修改**：
   ```properties
   # 启用双写
   rocksdbCQDoubleWriteEnable=true
   combineCQLoadingCQTypes=default;defaultRocksDB
   combineCQPreferCQType=default
   combineAssignOffsetCQType=default
   ```

2. **重启Broker**：重启后，新消息会同时写入File CQ和KV CQ

3. **验证双写**：使用`doCheckCqWriteProgress`方法检查双写进度，确保File CQ和KV CQ数据一致

#### 阶段2：历史数据迁移（可选，如果需要完整迁移）
如果需要将历史数据也迁移到KV CQ，可以通过recover机制：
- 利用`CombineConsumeQueueStore.verifyAndInitOffsetForAllStore`方法
- 从File CQ读取历史数据，写入KV CQ
- 或者等待自然过期（如果数据有TTL）

#### 阶段3：切换读取到KV CQ
1. **配置修改**：
   ```properties
   combineCQPreferCQType=defaultRocksDB  # 切换读取到KV CQ
   combineAssignOffsetCQType=default     # 仍使用File CQ分配offset
   ```

2. **滚动重启Broker**：逐个重启Broker，验证读取正常

3. **监控验证**：观察一段时间，确保消费正常

#### 阶段4：切换offset分配到KV CQ
1. **配置修改**：
   ```properties
   combineAssignOffsetCQType=defaultRocksDB  # 切换offset分配到KV CQ
   ```

2. **滚动重启Broker**

#### 阶段5：停止File CQ（可选）
如果确认不再需要File CQ：
1. **配置修改**：
   ```properties
   combineCQLoadingCQTypes=defaultRocksDB  # 只加载KV CQ
   ```

2. **重启Broker**：此时不再加载File CQ

### 优点
- 平滑迁移，风险低
- 可以随时回滚
- 支持逐步验证
- 新消息自动双写，无需额外迁移

### 缺点
- 迁移期间需要双倍存储空间
- 需要多次重启
- 历史数据迁移需要额外处理

---

## 方案二：配置直接切换方案

### 方案概述
直接修改配置，让Broker只使用KV CQ，通过recover机制从CommitLog重建KV CQ。

### 实施步骤

1. **配置修改**：
   ```properties
   rocksdbCQDoubleWriteEnable=false
   combineCQLoadingCQTypes=defaultRocksDB
   combineCQPreferCQType=defaultRocksDB
   combineAssignOffsetCQType=defaultRocksDB
   ```

2. **重启Broker**：
   - Broker启动时会从CommitLog重新构建KV CQ
   - 这个过程可能需要较长时间，取决于CommitLog大小

3. **验证**：确认KV CQ数据正确

### 优点
- 操作简单，一步到位
- 不需要双倍存储

### 缺点
- 启动时间较长（需要从CommitLog重建）
- 风险较高，无法回滚
- 需要停机时间

---

## 方案三：增量迁移方案

### 方案概述
新消息写入KV CQ，旧数据保留在File CQ，通过后台任务逐步迁移历史数据。

### 实施步骤

#### 阶段1：启用双写，但优先读取File CQ
```properties
rocksdbCQDoubleWriteEnable=true
combineCQLoadingCQTypes=default;defaultRocksDB
combineCQPreferCQType=default
combineAssignOffsetCQType=defaultRocksDB  # 新消息offset从KV CQ分配
```

#### 阶段2：实现后台迁移任务
需要开发一个迁移工具，从File CQ读取历史数据，写入KV CQ：
- 遍历所有topic和queueId
- 从File CQ读取CQ数据
- 写入KV CQ
- 记录迁移进度

#### 阶段3：迁移完成后切换读取
```properties
combineCQPreferCQType=defaultRocksDB
```

### 优点
- 可以控制迁移速度
- 不影响在线服务
- 可以暂停和恢复

### 缺点
- 需要开发迁移工具
- 迁移期间需要维护两套数据
- 实现复杂度较高

---

## 方案四：按Topic分批迁移方案

### 方案概述
利用Topic级别的CQ类型配置，逐个Topic迁移到KV CQ。

### 实施步骤

1. **为特定Topic启用KV CQ**：
   - 修改Topic配置，设置CQ类型为RocksDBCQ
   - 或者通过Topic属性配置

2. **启用双写**：
   ```properties
   rocksdbCQDoubleWriteEnable=true
   combineCQLoadingCQTypes=default;defaultRocksDB
   ```

3. **逐个Topic迁移**：
   - 先迁移低优先级Topic
   - 验证无误后迁移高优先级Topic

### 优点
- 风险分散，可以逐个验证
- 可以优先迁移重要Topic

### 缺点
- 需要Topic级别的配置支持
- 管理复杂度较高

---

## 推荐方案对比

| 方案 | 风险 | 复杂度 | 停机时间 | 存储开销 | 推荐场景 |
|------|------|--------|----------|----------|----------|
| **方案五：按Offset路由** | **极低** | **低** | **无** | **低** | **生产环境首选** |
| 方案一：双写+逐步切换 | 低 | 中 | 短（滚动重启） | 高（双写期间） | 需要完整迁移的场景 |
| 方案二：直接切换 | 高 | 低 | 长（重建CQ） | 低 | 测试环境或小规模集群 |
| 方案三：增量迁移 | 中 | 高 | 无 | 中 | 大规模历史数据场景 |
| 方案四：按Topic分批 | 低 | 高 | 短 | 中 | 多Topic场景 |

## 实施建议

1. **生产环境推荐使用方案一**，风险最低，可以平滑迁移
2. **迁移前准备**：
   - 备份数据
   - 准备回滚方案
   - 准备监控和告警
3. **迁移过程**：
   - 先在测试环境验证
   - 选择业务低峰期进行
   - 逐步推进，每个阶段充分验证
4. **验证检查**：
   - 使用`doCheckCqWriteProgress`检查双写一致性
   - 监控消费延迟和错误率
   - 检查offset分配是否正确

## 注意事项

1. **数据一致性**：确保双写期间File CQ和KV CQ数据一致
2. **性能影响**：双写会增加写入开销，需要评估性能影响
3. **存储空间**：双写期间需要双倍存储空间
4. **回滚准备**：保留File CQ数据，以便需要时回滚
5. **监控告警**：加强监控，及时发现异常

## 相关代码位置

- `CombineConsumeQueueStore`: `store/src/main/java/org/apache/rocketmq/store/queue/CombineConsumeQueueStore.java`
- `ConsumeQueueStore`: `store/src/main/java/org/apache/rocketmq/store/queue/ConsumeQueueStore.java`
- `RocksDBConsumeQueueStore`: `store/src/main/java/org/apache/rocketmq/store/queue/RocksDBConsumeQueueStore.java`
- 配置项: `store/src/main/java/org/apache/rocketmq/store/config/MessageStoreConfig.java`
