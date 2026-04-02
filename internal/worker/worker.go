package worker

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/izut/workflow-base/internal/client"
	"github.com/izut/workflow-base/internal/model"
)

// ============================================================================
// Worker工作节点核心结构体
// ============================================================================

// Worker 工作节点核心结构体
// 负责从任务队列获取任务、执行任务、发送心跳等功能
type Worker struct {
	nodeInfo    *model.NodeInfo          // 节点信息
	redisClient *client.RedisClient      // Redis客户端
	tsClient    *client.TableStoreClient // TableStore客户端(可选)
	config      *model.WorkerConfig      // 节点配置
	taskHandler TaskHandler              // 任务处理器
	stopChan    chan struct{}            // 停止信号通道
	wg          sync.WaitGroup           // 等待组，用于等待协程结束
	isRunning   bool                     // 运行状态标志
	mu          sync.Mutex               // 互斥锁，保护运行状态
}

// ============================================================================
// 任务处理器接口定义
// ============================================================================

// TaskHandler 任务处理器接口
// 用户可以实现此接口来处理具体的业务任务
type TaskHandler interface {
	// HandleTask 处理任务的业务逻辑
	// 参数:
	//   - ctx: 上下文对象，用于超时控制和取消
	//   - task: 任务对象，包含输入数据和执行信息
	//
	// 返回:
	//   - error: 处理失败时返回错误
	HandleTask(ctx context.Context, task *model.Task) error
}

// ============================================================================
// 构造函数
// ============================================================================

// NewWorker 创建工作节点实例
// 参数:
//   - nodeInfo: 节点信息对象
//   - redisClient: Redis客户端
//   - tsClient: TableStore客户端(可传nil)
//   - config: 节点配置
//   - handler: 任务处理器
//
// 返回: 初始化好的Worker指针
func NewWorker(nodeInfo *model.NodeInfo, redisClient *client.RedisClient, tsClient *client.TableStoreClient, config *model.WorkerConfig, handler TaskHandler) *Worker {
	return &Worker{
		nodeInfo:    nodeInfo,
		redisClient: redisClient,
		tsClient:    tsClient,
		config:      config,
		taskHandler: handler,
		stopChan:    make(chan struct{}),
	}
}

// ============================================================================
// 生命周期管理
// ============================================================================

// Start 启动工作节点
// 执行节点注册、启动任务处理循环和心跳循环
//
// 返回: 启动失败时返回错误
func (w *Worker) Start() error {
	// 加锁检查运行状态，防止重复启动
	w.mu.Lock()
	if w.isRunning {
		w.mu.Unlock()
		return nil
	}
	w.isRunning = true
	w.mu.Unlock()

	// 创建默认上下文
	ctx := context.Background()

	// 在Redis中注册节点
	if err := w.redisClient.RegisterNode(ctx, w.nodeInfo); err != nil {
		return err
	}

	// 如果配置了TableStore，也在TableStore中注册节点
	if w.tsClient != nil {
		if err := w.tsClient.RegisterNode(w.nodeInfo); err != nil {
			log.Printf("Failed to register node to TableStore: %v", err)
		}
	}

	// 启动任务处理协程
	w.wg.Add(1)
	go w.runTaskLoop()

	// 启动心跳协程
	w.wg.Add(1)
	go w.runHeartbeatLoop()

	log.Printf("Worker started: %s", w.nodeInfo.NodeName)
	return nil
}

// Stop 停止工作节点
// 停止任务处理和心跳，注销节点
//
// 返回: 停止过程中发生的错误
func (w *Worker) Stop() error {
	// 加锁检查运行状态
	w.mu.Lock()
	if !w.isRunning {
		w.mu.Unlock()
		return nil
	}
	w.isRunning = false
	w.mu.Unlock()

	// 发送停止信号
	close(w.stopChan)
	// 等待所有协程退出
	w.wg.Wait()

	// 创建上下文用于注销操作
	ctx := context.Background()

	// 从Redis注销节点
	if err := w.redisClient.UnregisterNode(ctx, w.nodeInfo.NodeName, w.nodeInfo.InstanceID); err != nil {
		log.Printf("Failed to unregister node from Redis: %v", err)
	}

	// 从TableStore注销节点(如果配置了)
	if w.tsClient != nil {
		if err := w.tsClient.UnregisterNode(w.nodeInfo.NodeName); err != nil {
			log.Printf("Failed to unregister node from TableStore: %v", err)
		}
	}

	log.Printf("Worker stopped: %s", w.nodeInfo.NodeName)
	return nil
}

// ============================================================================
// 任务处理循环
// ============================================================================

// runTaskLoop 任务处理主循环
// 持续从任务队列获取任务并处理
func (w *Worker) runTaskLoop() {
	defer w.wg.Done()

	// 获取配置的拉取间隔，默认5秒
	pollInterval := w.config.Worker.PollInterval
	if pollInterval == 0 {
		pollInterval = 5 * time.Second
	}

	log.Printf("Task loop started, poll interval: %v", pollInterval)

	// 主循环
	for {
		// 监听停止信号
		select {
		case <-w.stopChan:
			log.Printf("Task loop stopped")
			return
		default:
			// 执行任务拉取和处理
			w.pollAndProcessTask(pollInterval)
		}
	}
}

// pollAndProcessTask 拉取并处理单个任务
// 参数:
//   - pollInterval: 拉取超时时间
func (w *Worker) pollAndProcessTask(pollInterval time.Duration) {
	// 创建带超时的上下文
	ctx, cancel := context.WithTimeout(context.Background(), pollInterval)
	defer cancel()

	// 从Redis队列拉取任务
	task, err := w.redisClient.PullTask(ctx, w.nodeInfo.NodeName, pollInterval)
	if err != nil {
		log.Printf("Error pulling task: %v", err)
		time.Sleep(time.Second)
		return
	}

	// 如果没有任务，直接返回
	if task == nil {
		return
	}

	log.Printf("Received task: %s", task.TaskID)

	// 处理任务
	w.processTask(task)
}

// processTask 处理单个任务
// 包括状态更新、任务执行、结果回写等
// 参数:
//   - task: 要处理的任务
func (w *Worker) processTask(task *model.Task) {
	// 创建执行上下文
	ctx := context.Background()

	// 标记任务为运行中
	task.MarkRunning()
	// 更新任务状态到Redis
	if err := w.redisClient.UpdateTaskStatus(ctx, task); err != nil {
		log.Printf("Failed to update task status: %v", err)
	}

	// 增加节点负载计数
	w.nodeInfo.CurrentLoad++
	// 更新节点心跳(包含最新负载)
	w.redisClient.UpdateNodeHeartbeat(ctx, w.nodeInfo)

	// 调用任务处理器执行业务逻辑
	err := w.taskHandler.HandleTask(ctx, task)

	// 减少节点负载计数
	w.nodeInfo.CurrentLoad--

	// 根据执行结果更新任务状态
	if err != nil {
		log.Printf("Task failed: %s, error: %v", task.TaskID, err)
		task.MarkFailed(err.Error())
	} else {
		log.Printf("Task completed: %s", task.TaskID)
		task.MarkCompleted(task.OutputData)
	}

	// 更新最终任务状态
	if err := w.redisClient.UpdateTaskStatus(ctx, task); err != nil {
		log.Printf("Failed to update task status: %v", err)
	}
}

// ============================================================================
// 心跳循环
// ============================================================================

// runHeartbeatLoop 心跳发送主循环
// 定期向工作流控制中心发送心跳，更新节点状态
func (w *Worker) runHeartbeatLoop() {
	defer w.wg.Done()

	// 获取配置的心跳间隔，默认30秒
	heartbeatInterval := w.config.Worker.HeartbeatInterval
	if heartbeatInterval == 0 {
		heartbeatInterval = 30 * time.Second
	}

	// 创建定时器
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	log.Printf("Heartbeat loop started, interval: %v", heartbeatInterval)

	// 主循环
	for {
		select {
		case <-w.stopChan:
			log.Printf("Heartbeat loop stopped")
			return
		case <-ticker.C:
			// 发送心跳
			w.sendHeartbeat()
		}
	}
}

// sendHeartbeat 发送节点心跳
// 更新节点状态、工作负载和最后心跳时间
func (w *Worker) sendHeartbeat() {
	// 创建执行上下文
	ctx := context.Background()

	// 更新最后心跳时间为当前时间
	w.nodeInfo.LastHeartbeat = time.Now().UnixMilli()

	// 更新Redis中的节点心跳
	if err := w.redisClient.UpdateNodeHeartbeat(ctx, w.nodeInfo); err != nil {
		log.Printf("Failed to update heartbeat in Redis: %v", err)
	}

	// 更新TableStore中的节点心跳(如果配置了)
	if w.tsClient != nil {
		if err := w.tsClient.UpdateNodeHeartbeat(w.nodeInfo); err != nil {
			log.Printf("Failed to update heartbeat in TableStore: %v", err)
		}
	}
}

// ============================================================================
// 状态查询
// ============================================================================

// GetStatus 获取节点当前状态
// 用于监控接口，返回节点的实时状态信息
//
// 返回: 包含节点状态信息的map
func (w *Worker) GetStatus() map[string]interface{} {
	// 创建执行上下文
	ctx := context.Background()

	// 获取任务队列长度
	queueLength, _ := w.redisClient.GetQueueLength(ctx, w.nodeInfo.NodeName)

	// 返回状态信息
	return map[string]interface{}{
		"node_name":    w.nodeInfo.NodeName,    // 节点名称
		"node_type":    w.nodeInfo.NodeType,    // 节点类型
		"status":       w.nodeInfo.Status,      // 节点状态
		"current_load": w.nodeInfo.CurrentLoad, // 当前负载
		"max_capacity": w.nodeInfo.MaxCapacity, // 最大容量
		"queue_length": queueLength,            // 队列积压长度
		"is_running":   w.isRunning,            // 是否运行中
	}
}
