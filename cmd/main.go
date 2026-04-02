package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/izut/workflow-base/internal/client"
	"github.com/izut/workflow-base/internal/config"
	"github.com/izut/workflow-base/internal/model"
	"github.com/izut/workflow-base/internal/worker"
)

// ============================================================================
// 命令行参数定义
// ============================================================================

var (
	// 配置文件路径参数
	// 使用方法: --config /path/to/config.yaml
	configPath = flag.String("config", "config.yaml", "Path to configuration file")

	// 显示帮助信息参数
	// 使用方法: --help
	showHelp = flag.Bool("help", false, "Show help message")
)

// ============================================================================
// 默认任务处理器
// ============================================================================

// DefaultTaskHandler 默认任务处理器实现
// 这是一个简单的示例处理器，用户可以根据需要实现自己的业务逻辑
type DefaultTaskHandler struct{}

// HandleTask 处理任务的业务逻辑
// 参数:
//   - ctx: 上下文对象，用于超时控制和取消
//   - task: 任务对象，包含输入数据
//
// 返回:
//   - error: 处理失败时返回错误
func (h *DefaultTaskHandler) HandleTask(ctx context.Context, task *model.Task) error {
	// 打印任务处理日志
	log.Printf("Processing task %s: %+v", task.TaskID, task.InputData)

	// 设置任务输出数据
	// 在实际使用中，这里应该放置真正的业务处理逻辑
	task.OutputData = map[string]interface{}{
		"result":  "success",                     // 处理结果
		"message": "Task processed successfully", // 处理消息
		"task_id": task.TaskID,                   // 任务ID
	}

	// 返回nil表示任务处理成功
	return nil
}

// ============================================================================
// 主函数入口
// ============================================================================

// main 程序主入口
// 负责初始化配置、创建客户端、启动工作节点、处理退出信号等
func main() {
	// 解析命令行参数
	flag.Parse()

	// 如果指定了--help参数，显示帮助信息并退出
	if *showHelp {
		fmt.Println("Workflow Worker Plugin - 工作流控制中心节点插件")
		fmt.Println("\n使用方法:")
		fmt.Println("  workflow-worker --config <path>")
		fmt.Println("\n环境变量说明:")
		fmt.Println("  WORKER_NODE_NAME          - 节点唯一名称")
		fmt.Println("  WORKER_NODE_TYPE          - 节点类型")
		fmt.Println("  WORKER_HOST               - 节点主机地址")
		fmt.Println("  WORKER_PORT               - 节点服务端口")
		fmt.Println("  REDIS_ADDR               - Redis服务器地址")
		fmt.Println("  REDIS_PASSWORD           - Redis密码")
		fmt.Println("  REDIS_DB                 - Redis数据库编号")
		fmt.Println("  TABLESTORE_ENDPOINT      - TableStore服务端点")
		fmt.Println("  TABLESTORE_ACCESS_KEY_ID - TableStore访问密钥ID")
		fmt.Println("  TABLESTORE_ACCESS_KEY_SECRET - TableStore访问密钥")
		fmt.Println("  TABLESTORE_INSTANCE_NAME - TableStore实例名称")
		fmt.Println("  TABLESTORE_TABLE_NAME    - TableStore表名称")
		fmt.Println("  WORKER_POLL_INTERVAL     - 任务拉取间隔(秒)")
		fmt.Println("  WORKER_HEARTBEAT_INTERVAL - 心跳间隔(秒)")
		fmt.Println("\n使用示例:")
		fmt.Println("  # 使用配置文件启动")
		fmt.Println("  workflow-worker --config config.yaml")
		fmt.Println("")
		fmt.Println("  # 使用环境变量启动")
		fmt.Println("  export WORKER_NODE_NAME=my-worker")
		fmt.Println("  export REDIS_ADDR=localhost:6379")
		fmt.Println("  workflow-worker")
		return
	}

	// 加载配置
	// 优先从配置文件加载，配置文件不存在时从环境变量加载
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 打印启动日志
	log.Printf("Starting workflow worker: %s", cfg.NodeName)

	// 创建节点信息对象
	// 使用配置中的参数初始化节点信息
	nodeInfo := model.NewNodeInfo(
		cfg.NodeName,     // 节点名称
		cfg.NodeType,     // 节点类型
		cfg.Host,         // 主机地址
		cfg.Port,         // 服务端口
		cfg.Capabilities, // 节点能力列表
		cfg.MaxCapacity,  // 最大容量
		"v1.0.0",         // 节点版本号
	)

	// 创建Redis客户端
	// 使用配置中的Redis参数连接Redis服务器
	redisClient, err := client.NewRedisClient(
		cfg.Redis.Addr,     // Redis地址
		cfg.Redis.Password, // Redis密码
		cfg.Redis.DB,       // Redis数据库编号
	)
	if err != nil {
		// Redis连接失败是致命错误，无法继续运行
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	// 确保程序退出时关闭Redis连接
	defer redisClient.Close()

	// 创建TableStore客户端(可选)
	// 只有配置了TableStore端点时才创建
	var tsClient *client.TableStoreClient
	if cfg.TableStore.Endpoint != "" {
		tsClient, err = client.NewTableStoreClient(
			cfg.TableStore.Endpoint,        // TableStore端点
			cfg.TableStore.AccessKeyID,     // AccessKey ID
			cfg.TableStore.AccessKeySecret, // AccessKey Secret
			cfg.TableStore.InstanceName,    // 实例名称
			cfg.TableStore.TableName,       // 表名称
		)
		if err != nil {
			// TableStore连接失败只是警告，不影响主流程
			log.Printf("Warning: Failed to connect to TableStore: %v", err)
			tsClient = nil
		}
	}

	// 创建默认任务处理器
	// 用户可以替换为自己的任务处理器实现
	taskHandler := &DefaultTaskHandler{}

	// 创建工作节点实例
	// 传入节点信息、客户端、配置和任务处理器
	w := worker.NewWorker(nodeInfo, redisClient, tsClient, cfg, taskHandler)

	// 启动工作节点
	// 这将开始任务拉取循环和心跳循环
	if err := w.Start(); err != nil {
		log.Fatalf("Failed to start worker: %v", err)
	}

	// 创建信号通道
	// 用于接收系统信号(SIGINT, SIGTERM)
	sigChan := make(chan os.Signal, 1)
	// 注册信号处理器
	// 监听Ctrl+C(SIGINT)和终止信号(SIGTERM)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 阻塞等待信号
	// 当收到信号时，继续执行后面的清理代码
	<-sigChan

	// 收到退出信号，打印日志
	log.Println("Shutting down...")

	// 停止工作节点
	// 这将停止任务拉取、心跳，并注销节点
	if err := w.Stop(); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}

	// 打印退出日志
	log.Println("Worker stopped")
}

// ============================================================================
// 配置加载函数
// ============================================================================

// loadConfig 加载节点配置
// 优先尝试从配置文件加载，配置文件不存在时从环境变量加载
//
// 返回:
//   - *model.WorkerConfig: 加载的配置对象
//   - error: 加载失败时返回错误
func loadConfig() (*model.WorkerConfig, error) {
	// 创建配置加载器
	loader := config.NewConfigLoader()

	// 检查配置文件是否存在
	if _, err := os.Stat(*configPath); err == nil {
		// 配置文件存在，从文件加载
		log.Printf("Loading config from file: %s", *configPath)
		return loader.LoadFromFile(*configPath)
	}

	// 配置文件不存在，从环境变量加载
	log.Println("Config file not found, loading from environment variables")
	return loader.LoadFromEnv()
}
