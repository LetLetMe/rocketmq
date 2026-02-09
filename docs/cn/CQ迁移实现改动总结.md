# CQ迁移实现改动总结

## 概述

本文档总结了File CQ到KV CQ迁移功能的实现改动，包括配置简化、topic灰度支持、offset路由机制和观测工具的实现。

## 功能描述

实现了按offset路由的CQ迁移方案：
- 新消息只写入KV CQ（对于灰度topic）
- 读取时根据offset自动路由：旧offset从File CQ读取，新offset从KV CQ读取
- 支持topic级别的灰度配置，支持通配符匹配

## 配置项说明

### 1. combineCQWriteOnlyRocksDB
- **类型**: boolean
- **默认值**: false
- **说明**: 主开关，控制是否启用只写KV CQ模式

### 2. combineCQWriteOnlyRocksDBTopics
- **类型**: String
- **默认值**: ""（空字符串）
- **说明**: Topic灰度列表，逗号分隔，支持通配符（`*`和`?`）
- **示例**: `"test*,prod-*,topic1"`

**注意**：
- 如果`combineCQWriteOnlyRocksDB=true`但`combineCQWriteOnlyRocksDBTopics`为空，则所有topic都会启用只写KV CQ模式
- 如果`combineCQWriteOnlyRocksDB=false`，则`combineCQWriteOnlyRocksDBTopics`配置无效

## 修改的文件列表

### 1. 新增文件

#### 1.1 Topic灰度匹配工具类
- **文件**: `store/src/main/java/org/apache/rocketmq/store/util/TopicGrayscaleMatcher.java`
- **说明**: 实现通配符匹配逻辑，支持`*`（匹配任意字符序列）和`?`（匹配单个字符）
- **主要方法**:
  - `matches(String topic)`: 检查topic是否匹配灰度列表中的任何模式
  - `matchesPattern(String topic, String pattern)`: 通配符匹配实现

#### 1.2 CQ Offset路由信息数据结构
- **文件**: `common/src/main/java/org/apache/rocketmq/common/CQOffsetRouteInfo.java`
- **说明**: 定义观测工具返回的数据结构
- **包含信息**:
  - Topic级别的offset范围（File CQ和KV CQ）
  - Queue级别的offset范围
  - 路由切换点（File CQ的maxOffset）

#### 1.3 Admin请求头
- **文件**: `remoting/src/main/java/org/apache/rocketmq/remoting/protocol/header/CheckCQOffsetRouteRequestHeader.java`
- **说明**: 定义观测工具的请求头

#### 1.4 观测工具命令
- **文件**: `tools/src/main/java/org/apache/rocketmq/tools/command/queue/CheckCQOffsetRouteCommand.java`
- **说明**: 命令行工具，用于查看CQ offset路由信息

### 2. 修改的文件

#### 2.1 配置类
- **文件**: `store/src/main/java/org/apache/rocketmq/store/config/MessageStoreConfig.java`
- **修改内容**:
  - 添加`combineCQWriteOnlyRocksDBTopics`配置项
  - 添加`isTopicInGrayscaleList(String topic)`方法，用于检查topic是否在灰度列表中
  - 添加`StringUtils`导入

#### 2.2 CombineConsumeQueueStore
- **文件**: `store/src/main/java/org/apache/rocketmq/store/queue/CombineConsumeQueueStore.java`
- **修改内容**:
  - **putMessagePositionInfoWrapper**: 添加topic灰度检查，只对灰度topic写入KV CQ
  - **getMaxOffset**: 添加topic灰度检查，灰度topic的maxOffset从KV CQ获取
  - **getMinOffsetInQueue**: 添加topic灰度检查，灰度topic的minOffset从File CQ获取（如果存在）
  - **getOffsetInQueueByTime**: 完善实现，支持根据timestamp在File CQ和KV CQ中查找
  - **selectReadStoreByOffset**: 添加topic灰度检查
  - **getCQOffsetRouteInfo**: 新增方法，用于获取offset路由信息供观测工具使用

#### 2.3 DefaultMessageStore
- **文件**: `store/src/main/java/org/apache/rocketmq/store/DefaultMessageStore.java`
- **修改内容**:
  - **getMessageStoreTimeStamp**: 修改为使用`findConsumeQueue(topic, queueId, offset)`以支持offset路由

#### 2.4 RequestCode
- **文件**: `remoting/src/main/java/org/apache/rocketmq/remoting/protocol/RequestCode.java`
- **修改内容**:
  - 添加`CHECK_CQ_OFFSET_ROUTE = 356`请求码

#### 2.5 AdminBrokerProcessor
- **文件**: `broker/src/main/java/org/apache/rocketmq/broker/processor/AdminBrokerProcessor.java`
- **修改内容**:
  - 添加`checkCQOffsetRoute`方法处理观测请求
  - 添加相关import

#### 2.6 MQAdminExt接口和实现
- **文件**: 
  - `tools/src/main/java/org/apache/rocketmq/tools/admin/MQAdminExt.java`
  - `tools/src/main/java/org/apache/rocketmq/tools/admin/DefaultMQAdminExt.java`
  - `tools/src/main/java/org/apache/rocketmq/tools/admin/DefaultMQAdminExtImpl.java`
- **修改内容**:
  - 添加`checkCQOffsetRoute`方法声明和实现

#### 2.7 MQClientAPIImpl
- **文件**: `client/src/main/java/org/apache/rocketmq/client/impl/MQClientAPIImpl.java`
- **修改内容**:
  - 添加`checkCQOffsetRoute`方法实现，发送请求到broker

#### 2.8 MQAdminStartup
- **文件**: `tools/src/main/java/org/apache/rocketmq/tools/command/MQAdminStartup.java`
- **修改内容**:
  - 注册`CheckCQOffsetRouteCommand`命令

## 实现细节

### 1. Offset路由逻辑

**写入逻辑**：
- 检查topic是否在灰度列表中
- 如果在灰度列表中，只写入KV CQ
- 如果不在灰度列表中，写入所有store（默认行为）

**读取路由逻辑**：
- 检查topic是否在灰度列表中
- 如果不在灰度列表中，使用currentReadStore
- 如果在灰度列表中：
  1. 获取File CQ的maxOffset作为切换点
  2. 如果请求的offset < File CQ的maxOffset，从File CQ读取
  3. 如果请求的offset >= File CQ的maxOffset，从KV CQ读取

**Offset范围获取**：
- `getMinOffsetInQueue`: 灰度topic返回File CQ的minOffset（如果存在），否则返回KV CQ的minOffset
- `getMaxOffset`: 灰度topic返回KV CQ的maxOffset

### 2. Topic灰度匹配逻辑

**通配符支持**：
- `*`: 匹配任意字符序列（包括空序列）
- `?`: 匹配单个字符

**匹配规则**：
- 如果`combineCQWriteOnlyRocksDB=false`，所有topic都不在灰度列表中
- 如果`combineCQWriteOnlyRocksDB=true`但`combineCQWriteOnlyRocksDBTopics`为空，所有topic都在灰度列表中
- 如果`combineCQWriteOnlyRocksDB=true`且`combineCQWriteOnlyRocksDBTopics`不为空，只有匹配的topic在灰度列表中

**示例**：
- `"test*"` 匹配: `test`, `test1`, `test-topic`, `test123`
- `"prod-*"` 匹配: `prod-topic`, `prod-queue`, `prod-123`
- `"topic1,topic2"` 匹配: `topic1`, `topic2`

### 3. 边界情况处理

1. **File CQ为空时（新topic）**：
   - minOffset返回KV CQ的minOffset
   - maxOffset返回KV CQ的maxOffset
   - 所有offset都从KV CQ读取

2. **KV CQ为空时（未启用时）**：
   - 使用currentReadStore（通常是File CQ）

3. **Topic不在灰度列表但开关开启时**：
   - 使用默认行为（写入所有store，读取使用currentReadStore）

4. **Topic在灰度列表但开关关闭时**：
   - 使用默认行为（写入所有store，读取使用currentReadStore）

5. **只有一个store加载时**：
   - 直接使用该store，无需路由

## 使用方法

### 配置示例

```properties
# 启用只写KV CQ模式
combineCQWriteOnlyRocksDB=true

# 配置灰度topic列表（支持通配符）
combineCQWriteOnlyRocksDBTopics=test*,prod-*,important-topic

# 需要同时加载File CQ和KV CQ
rocksdbCQDoubleWriteEnable=true
combineCQLoadingCQTypes=default;defaultRocksDB
```

### 观测工具使用示例

#### 查看所有topic的路由信息
```bash
mqadmin checkCQOffsetRoute -n localhost:9876 -b 127.0.0.1:10911
```

#### 查看指定topic的路由信息（包含queue详情）
```bash
mqadmin checkCQOffsetRoute -n localhost:9876 -b 127.0.0.1:10911 -t test-topic
```

#### 查看集群中所有broker的路由信息
```bash
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

## 实现完备性检查

### 已实现的读取路径

1. **getMessage** ✓
   - 使用`findConsumeQueue(topic, queueId, offset)`支持offset路由
   - 已完整实现

2. **getOffsetInQueueByTime** ✓
   - 支持根据timestamp在File CQ和KV CQ中查找
   - 已完整实现

3. **getMessageStoreTimeStamp** ✓
   - 使用`findConsumeQueue(topic, queueId, offset)`支持offset路由
   - 已完整实现

4. **queryMessage** ✓
   - 通过IndexFile查询，直接读取CommitLog，不依赖CQ
   - 无需特殊处理

### 已实现的写入路径

1. **putMessagePositionInfoWrapper** ✓
   - 检查topic是否在灰度列表中
   - 灰度topic只写入KV CQ
   - 已完整实现

### Offset分配

- **assignQueueOffset**和**increaseQueueOffset**使用`assignOffsetStore`
- `assignOffsetStore`通过`combineAssignOffsetCQType`配置
- 建议配置为`defaultRocksDB`，使灰度topic的offset从KV CQ分配

## 注意事项

### 迁移建议

1. **逐步灰度**：
   - 先配置少量低优先级topic进行测试（如：`combineCQWriteOnlyRocksDBTopics=test-topic`）
   - 验证无误后逐步扩大灰度范围（如：`combineCQWriteOnlyRocksDBTopics=test*,prod-*`）
   - 最后可以设置为空字符串（所有topic）

2. **监控观察**：
   - 使用观测工具定期检查offset路由状态
   - 监控消费延迟和错误率
   - 关注File CQ和KV CQ的offset范围变化

3. **数据清理**：
   - File CQ数据会随着消息过期自动清理
   - 如果数据有TTL，可以等待File CQ数据自然过期
   - 过期后可以停止加载File CQ

### 常见问题

1. **Q: 如何判断一个offset应该从哪个CQ读取？**
   - A: 如果offset < File CQ的maxOffset，从File CQ读取；否则从KV CQ读取

2. **Q: 如果File CQ为空（新topic），会怎样？**
   - A: 所有offset都从KV CQ读取，minOffset和maxOffset都来自KV CQ

3. **Q: 如何查看某个topic的路由状态？**
   - A: 使用`mqadmin checkCQOffsetRoute -n <nameserver> -b <broker> -t <topic>`命令

4. **Q: 通配符匹配是否区分大小写？**
   - A: 是的，匹配是区分大小写的

5. **Q: 修改灰度列表后需要重启broker吗？**
   - A: 是的，配置修改需要重启broker生效

6. **Q: 如果只配置了combineCQWriteOnlyRocksDB=true，但没有配置combineCQWriteOnlyRocksDBTopics会怎样？**
   - A: 所有topic都会启用只写KV CQ模式

7. **Q: assignQueueOffset使用的是哪个store？**
   - A: 使用`assignOffsetStore`，通过`combineAssignOffsetCQType`配置，建议设置为`defaultRocksDB`

## 相关文档

- [CQ迁移方案](./CQ迁移方案.md): 详细的迁移方案说明
- [CombineConsumeQueueStore源码](../store/src/main/java/org/apache/rocketmq/store/queue/CombineConsumeQueueStore.java)
- [TopicGrayscaleMatcher源码](../store/src/main/java/org/apache/rocketmq/store/util/TopicGrayscaleMatcher.java)
