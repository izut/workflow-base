package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/izut/workflow-base/internal/model"
	"github.com/redis/go-redis/v9"
)

// ============================================================================
// Redis客户端结构体定义
// ============================================================================

// RedisClient Redis客户端封装
// 提供对Redis的操作，包括任务队列、状态管理、心跳等功能
type RedisClient struct {
	client *redis.Client // 底层redis客户端连接
}

// ============================================================================
// 构造函数
// ============================================================================

// NewRedisClient 创建Redis客户端连接
// 参数:
//   - addr: Redis地址，格式: host:port
//   - password: Redis密码，空字符串表示无需密码
//   - db: Redis数据库编号(0-15)
//
// 返回:
//   - *RedisClient: Redis客户端指针
//   - error: 连接失败时返回错误
func NewRedisClient(addr, password string, db int) (*RedisClient, error) {
	// 创建Redis客户端连接，配置连接池参数
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		PoolSize:     100, // 最大连接数，默认等于CPU核数*10
		MinIdleConns: 10,  // 最小空闲连接数
	})

	// 创建5秒超时的上下文用于测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 测试Redis连接是否成功
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	return &RedisClient{client: client}, nil
}

// Close 关闭Redis连接
// 返回: 关闭过程中的错误信息
func (r *RedisClient) Close() error {
	return r.client.Close()
}

// ============================================================================
// Key生成函数 - 统一管理所有Redis Key的命名规范
// ============================================================================

// GetTaskQueueKey 获取任务队列的Key
// 格式: queue:node:task:{node_name}
// 参数: nodeName - 节点名称
// 返回: 完整的Redis Key字符串
func (r *RedisClient) GetTaskQueueKey(nodeName string) string {
	return fmt.Sprintf("queue:node:task:%s", nodeName)
}

// GetTaskStatusKey 获取任务状态的Key
// 格式: task:status:{task_id}
// 参数: taskID - 任务唯一标识
// 返回: 完整的Redis Key字符串
func (r *RedisClient) GetTaskStatusKey(taskID string) string {
	return fmt.Sprintf("task:status:%s", taskID)
}

// GetNodeStatusKey 获取节点状态的Key
// 格式: node:status:{node_name}
// 参数: nodeName - 节点名称
// 返回: 完整的Redis Key字符串
func (r *RedisClient) GetNodeStatusKey(nodeName string) string {
	return fmt.Sprintf("node:status:%s", nodeName)
}

// GetNodeHeartbeatKey 获取节点心跳的Key
// 格式: heartbeat:node:{node_name}
// 参数: nodeName - 节点名称
// 返回: 完整的Redis Key字符串
func (r *RedisClient) GetNodeHeartbeatKey(nodeName string) string {
	return fmt.Sprintf("heartbeat:node:%s", nodeName)
}

// GetWorkflowStatusKey 获取工作流状态的Key
// 格式: workflow:status:{instance_id}
// 参数: instanceID - 工作流实例ID
// 返回: 完整的Redis Key字符串
func (r *RedisClient) GetWorkflowStatusKey(instanceID string) string {
	return fmt.Sprintf("workflow:status:%s", instanceID)
}

// GetNodeVersionKey 获取节点状态版本的Key
// 用于实现乐观锁机制
// 参数: nodeName - 节点名称
// 返回: 完整的Redis Key字符串
func (r *RedisClient) GetNodeVersionKey(nodeName string) string {
	return fmt.Sprintf("version:node:%s", nodeName)
}

// GetWorkflowVersionKey 获取工作流状态版本的Key
// 用于实现乐观锁机制
// 参数: instanceID - 工作流实例ID
// 返回: 完整的Redis Key字符串
func (r *RedisClient) GetWorkflowVersionKey(instanceID string) string {
	return fmt.Sprintf("version:workflow:%s", instanceID)
}

// ============================================================================
// 任务队列操作
// ============================================================================

// PullTask 从任务队列中拉取任务
// 使用BRPOP阻塞等待，直到有任务或超时
// 参数:
//   - ctx: 上下文对象
//   - nodeName: 节点名称，用于确定队列
//   - timeout: 阻塞超时时间
//
// 返回:
//   - *model.Task: 拉取到的任务指针，无任务时返回nil
//   - error: 操作失败时返回错误
func (r *RedisClient) PullTask(ctx context.Context, nodeName string, timeout time.Duration) (*model.Task, error) {
	// 获取任务队列Key
	queueKey := r.GetTaskQueueKey(nodeName)

	// 使用BRPOP从队列尾部阻塞获取元素
	result, err := r.client.BRPop(ctx, timeout, queueKey).Result()
	if err != nil {
		// 如果是超时(队列为空)，返回nil而非错误
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to pull task: %w", err)
	}

	// BRPop返回[queue_name, element]，检查是否有数据
	if len(result) < 2 {
		return nil, nil
	}

	// 解析任务JSON数据
	var task model.Task
	if err := json.Unmarshal([]byte(result[1]), &task); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task: %w", err)
	}

	return &task, nil
}

// ============================================================================
// 任务状态管理
// ============================================================================

// UpdateTaskStatus 更新任务状态
// 将任务状态存储到Redis Hash中
// 参数:
//   - ctx: 上下文对象
//   - task: 任务对象，包含最新状态信息
//
// 返回: 操作失败时返回错误
func (r *RedisClient) UpdateTaskStatus(ctx context.Context, task *model.Task) error {
	// 获取任务状态Key
	statusKey := r.GetTaskStatusKey(task.TaskID)

	// 构建状态数据
	data := map[string]interface{}{
		"task_id":       task.TaskID,            // 任务ID
		"node_name":     task.NodeName,          // 节点名称
		"execution_id":  task.ExecutionID,       // 执行ID
		"instance_id":   task.InstanceID,        // 实例ID
		"workflow_id":   task.WorkflowID,        // 工作流ID
		"node_id":       task.NodeID,            // 节点ID
		"status":        task.Status,            // 任务状态
		"progress":      task.Progress,          // 任务进度
		"error_message": task.ErrorMessage,      // 错误信息
		"started_at":    task.StartedAt,         // 开始时间
		"completed_at":  task.CompletedAt,       // 完成时间
		"retry_count":   task.RetryCount,        // 重试次数
		"updated_at":    time.Now().UnixMilli(), // 更新时间
	}

	// 序列化输入数据
	if task.InputData != nil {
		inputJSON, _ := json.Marshal(task.InputData)
		data["input_data"] = string(inputJSON)
	}

	// 序列化输出数据
	if task.OutputData != nil {
		outputJSON, _ := json.Marshal(task.OutputData)
		data["output_data"] = string(outputJSON)
	}

	// 将map转换为HSET所需的参数格式
	fields := make([]interface{}, 0, len(data)*2)
	for k, v := range data {
		fields = append(fields, k, v)
	}

	// 存储到Redis Hash
	if err := r.client.HSet(ctx, statusKey, fields...).Err(); err != nil {
		return fmt.Errorf("failed to update task status: %w", err)
	}

	// 设置24小时过期时间
	if err := r.client.Expire(ctx, statusKey, 24*time.Hour).Err(); err != nil {
		return fmt.Errorf("failed to set expiry: %w", err)
	}

	return nil
}

// GetTaskStatus 获取任务状态
// 参数:
//   - ctx: 上下文对象
//   - taskID: 任务唯一标识
//
// 返回:
//   - map[string]string: 任务状态数据
//   - error: 操作失败时返回错误
func (r *RedisClient) GetTaskStatus(ctx context.Context, taskID string) (map[string]string, error) {
	statusKey := r.GetTaskStatusKey(taskID)
	result, err := r.client.HGetAll(ctx, statusKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get task status: %w", err)
	}
	return result, nil
}

// ============================================================================
// 节点心跳和状态管理
// ============================================================================

// UpdateNodeHeartbeat 更新节点心跳信息
// 同时更新心跳Hash和状态Hash，并递增版本号
// 参数:
//   - ctx: 上下文对象
//   - nodeInfo: 节点信息
//
// 返回: 操作失败时返回错误
func (r *RedisClient) UpdateNodeHeartbeat(ctx context.Context, nodeInfo *model.NodeInfo) error {
	// 获取相关Key
	heartbeatKey := r.GetNodeHeartbeatKey(nodeInfo.NodeName)
	statusKey := r.GetNodeStatusKey(nodeInfo.NodeName)
	versionKey := r.GetNodeVersionKey(nodeInfo.NodeName)

	// 获取当前时间戳
	now := time.Now().UnixMilli()

	// 更新心跳信息到Hash
	if err := r.client.HSet(ctx, heartbeatKey,
		"node_name", nodeInfo.NodeName,
		"node_type", nodeInfo.NodeType,
		"status", nodeInfo.Status,
		"last_heartbeat", now,
		"current_load", nodeInfo.CurrentLoad,
		"version", nodeInfo.Version,
	).Err(); err != nil {
		return fmt.Errorf("failed to update heartbeat: %w", err)
	}

	// 设置心跳2分钟过期(用于检测节点离线)
	if err := r.client.Expire(ctx, heartbeatKey, 120*time.Second).Err(); err != nil {
		return fmt.Errorf("failed to set heartbeat expiry: %w", err)
	}

	// 更新节点状态
	if err := r.client.HSet(ctx, statusKey,
		"status", nodeInfo.Status,
		"current_load", nodeInfo.CurrentLoad,
		"max_capacity", nodeInfo.MaxCapacity,
	).Err(); err != nil {
		return fmt.Errorf("failed to update node status: %w", err)
	}

	// 递增版本号，用于通知订阅者状态已更新
	r.client.Incr(ctx, versionKey)

	return nil
}

// GetNodeStatus 获取节点状态
// 参数:
//   - ctx: 上下文对象
//   - nodeName: 节点名称
//
// 返回:
//   - map[string]string: 节点状态数据
//   - error: 操作失败时返回错误
func (r *RedisClient) GetNodeStatus(ctx context.Context, nodeName string) (map[string]string, error) {
	statusKey := r.GetNodeStatusKey(nodeName)
	result, err := r.client.HGetAll(ctx, statusKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get node status: %w", err)
	}
	return result, nil
}

// RegisterNode 注册节点
// 将节点信息写入Redis，包括心跳Hash和状态Hash
// 参数:
//   - ctx: 上下文对象
//   - nodeInfo: 节点信息
//
// 返回: 操作失败时返回错误
func (r *RedisClient) RegisterNode(ctx context.Context, nodeInfo *model.NodeInfo) error {
	// 获取相关Key
	heartbeatKey := r.GetNodeHeartbeatKey(nodeInfo.NodeName)
	statusKey := r.GetNodeStatusKey(nodeInfo.NodeName)

	// 获取当前时间戳
	now := time.Now().UnixMilli()

	// 序列化节点能力列表
	capabilitiesJSON, _ := json.Marshal(nodeInfo.Capabilities)

	// 写入心跳信息
	if err := r.client.HSet(ctx, heartbeatKey,
		"node_name", nodeInfo.NodeName,
		"node_type", nodeInfo.NodeType,
		"host", nodeInfo.Host,
		"port", nodeInfo.Port,
		"status", nodeInfo.Status,
		"capabilities", string(capabilitiesJSON),
		"current_load", nodeInfo.CurrentLoad,
		"max_capacity", nodeInfo.MaxCapacity,
		"version", nodeInfo.Version,
		"registered_at", now,
		"last_heartbeat", now,
	).Err(); err != nil {
		return fmt.Errorf("failed to register node: %w", err)
	}

	// 设置2分钟过期
	if err := r.client.Expire(ctx, heartbeatKey, 120*time.Second).Err(); err != nil {
		return fmt.Errorf("failed to set expiry: %w", err)
	}

	// 写入节点状态
	if err := r.client.HSet(ctx, statusKey,
		"status", nodeInfo.Status,
		"current_load", nodeInfo.CurrentLoad,
		"max_capacity", nodeInfo.MaxCapacity,
	).Err(); err != nil {
		return fmt.Errorf("failed to set node status: %w", err)
	}

	return nil
}

// UnregisterNode 注销节点
// 删除节点的心跳Hash和状态Hash
// 参数:
//   - ctx: 上下文对象
//   - nodeName: 节点名称
//
// 返回: 操作失败时返回错误
func (r *RedisClient) UnregisterNode(ctx context.Context, nodeName string) error {
	heartbeatKey := r.GetNodeHeartbeatKey(nodeName)
	statusKey := r.GetNodeStatusKey(nodeName)

	// 删除相关Key
	if err := r.client.Del(ctx, heartbeatKey, statusKey).Err(); err != nil {
		return fmt.Errorf("failed to unregister node: %w", err)
	}

	return nil
}

// ============================================================================
// 工作流状态管理
// ============================================================================

// UpdateWorkflowStatus 更新工作流状态
// 参数:
//   - ctx: 上下文对象
//   - instanceID: 工作流实例ID
//   - status: 工作流状态
//   - progress: 执行进度
//   - currentNode: 当前执行的节点ID
//
// 返回: 操作失败时返回错误
func (r *RedisClient) UpdateWorkflowStatus(ctx context.Context, instanceID string, status string, progress float64, currentNode string) error {
	workflowKey := r.GetWorkflowStatusKey(instanceID)
	versionKey := r.GetWorkflowVersionKey(instanceID)

	now := time.Now().UnixMilli()

	// 更新工作流状态
	if err := r.client.HSet(ctx, workflowKey,
		"status", status,
		"progress", progress,
		"current_node_id", currentNode,
		"updated_at", now,
	).Err(); err != nil {
		return fmt.Errorf("failed to update workflow status: %w", err)
	}

	// 设置24小时过期
	if err := r.client.Expire(ctx, workflowKey, 24*time.Hour).Err(); err != nil {
		return fmt.Errorf("failed to set expiry: %w", err)
	}

	// 递增版本号
	r.client.Incr(ctx, versionKey)

	return nil
}

// GetWorkflowStatus 获取工作流状态
// 参数:
//   - ctx: 上下文对象
//   - instanceID: 工作流实例ID
//
// 返回:
//   - map[string]string: 工作流状态数据
//   - error: 操作失败时返回错误
func (r *RedisClient) GetWorkflowStatus(ctx context.Context, instanceID string) (map[string]string, error) {
	workflowKey := r.GetWorkflowStatusKey(instanceID)
	result, err := r.client.HGetAll(ctx, workflowKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow status: %w", err)
	}
	return result, nil
}

// ============================================================================
// 队列长度查询
// ============================================================================

// GetQueueLength 获取任务队列长度
// 用于监控队列积压情况
// 参数:
//   - ctx: 上下文对象
//   - nodeName: 节点名称
//
// 返回:
//   - int64: 队列长度
//   - error: 操作失败时返回错误
func (r *RedisClient) GetQueueLength(ctx context.Context, nodeName string) (int64, error) {
	queueKey := r.GetTaskQueueKey(nodeName)
	length, err := r.client.LLen(ctx, queueKey).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get queue length: %w", err)
	}
	return length, nil
}

// ============================================================================
// 任务重试
// ============================================================================

// RepushTask 将任务重新放回队列
// 用于任务执行失败后的重试
// 参数:
//   - ctx: 上下文对象
//   - task: 要重试的任务
//
// 返回: 操作失败时返回错误
func (r *RedisClient) RepushTask(ctx context.Context, task *model.Task) error {
	queueKey := r.GetTaskQueueKey(task.NodeName)

	// 序列化任务数据
	taskJSON, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	// 重新放入队列头部
	if err := r.client.LPush(ctx, queueKey, taskJSON).Err(); err != nil {
		return fmt.Errorf("failed to repush task: %w", err)
	}

	return nil
}
