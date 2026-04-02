# Workflow Worker Plugin

工作流控制中心节点插件 - 用于从工作流控制中心获取任务并执行

## 功能特性

- 从 Redis 队列获取任务
- 定期向工作流控制中心发送心跳
- 支持任务状态更新
- 支持 TableStore 节点注册（可选）
- 支持配置文件和命令行参数
- 支持环境变量配置

## 快速开始

### 方式一：使用配置文件

1. 创建配置文件 `config.yaml`:

```yaml
node_name: "my-worker-001"
node_type: "data_processor"
host: "192.168.1.100"
port: 8080

capabilities:
  - "data_clean"
  - "data_transform"

max_capacity: 10

redis:
  addr: "localhost:6379"
  password: ""
  db: 0

worker:
  poll_interval: 5
  heartbeat_interval: 30
```

2. 启动节点:

```bash
./workflow-worker --config config.yaml
```

### 方式二：使用环境变量

```bash
export WORKER_NODE_NAME="my-worker-001"
export WORKER_NODE_TYPE="data_processor"
export WORKER_HOST="192.168.1.100"
export REDIS_ADDR="localhost:6379"

./workflow-worker
```

## 配置说明

### 节点配置

| 参数 | 说明 | 默认值 |
|------|------|--------|
| node_name | 节点唯一名称 | - |
| node_type | 节点类型 | generic |
| host | 节点主机地址 | localhost |
| port | 节点服务端口 | 8080 |
| capabilities | 节点能力列表 | [] |
| max_capacity | 最大并发任务数 | 10 |

### Redis 配置

| 参数 | 说明 | 默认值 |
|------|------|--------|
| redis.addr | Redis 地址 | localhost:6379 |
| redis.password | Redis 密码 | - |
| redis.db | Redis 数据库编号 | 0 |

### TableStore 配置（可选）

| 参数 | 说明 | 默认值 |
|------|------|--------|
| tablestore.endpoint | TableStore 端点 | - |
| tablestore.access_key_id | AccessKey ID | - |
| tablestore.access_key_secret | AccessKey Secret | - |
| tablestore.instance_name | 实例名称 | - |
| tablestore.table_name | 表名称 | worker_node |

### 工作线程配置

| 参数 | 说明 | 默认值 |
|------|------|--------|
| worker.poll_interval | 任务拉取间隔(秒) | 5 |
| worker.heartbeat_interval | 心跳间隔(秒) | 30 |
| worker.max_retries | 最大重试次数 | 3 |
| worker.batch_size | 批量处理大小 | 1 |

## Redis 数据结构

### 任务队列

- Key: `queue:node:task:{node_name}`
- 数据类型: List
- 操作: `LPUSH` 入队，`BRPOP` 出队

### 任务状态

- Key: `task:status:{task_id}`
- 数据类型: Hash

### 节点心跳

- Key: `heartbeat:node:{node_name}`
- 数据类型: Hash

### 节点状态

- Key: `node:status:{node_name}`
- 数据类型: Hash

## 任务处理

节点通过实现 `TaskHandler` 接口来处理任务:

```go
type TaskHandler interface {
    HandleTask(ctx context.Context, task *types.Task) error
}
```

示例:

```go
type MyTaskHandler struct{}

func (h *MyTaskHandler) HandleTask(ctx context.Context, task *types.Task) error {
    log.Printf("Processing task: %s", task.TaskID)
    
    // 处理业务逻辑
    task.OutputData = map[string]interface{}{
        "result": "success",
    }
    
    return nil
}
```

## 状态流转

```
Pending → Running → Completed
                    → Failed
                    → Cancelled
```

## 心跳机制

节点启动时向 Redis 注册，定期发送心跳更新节点状态。工作流控制中心通过检查心跳判断节点是否在线。

## 构建

```bash
cd worker-plugin
go build -o workflow-worker ./cmd
```

## License

MIT License
