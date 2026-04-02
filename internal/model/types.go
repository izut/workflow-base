package model

import (
	"fmt"
	"time"
)

func generateInstanceID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixMilli(), time.Now().Nanosecond()%1000)
}

// ============================================================================
// 任务状态常量定义
// ============================================================================

// 任务状态枚举
const (
	TaskStatusPending   = "pending"   // 任务等待中
	TaskStatusRunning   = "running"   // 任务执行中
	TaskStatusCompleted = "completed" // 任务已完成
	TaskStatusFailed    = "failed"    // 任务执行失败
	TaskStatusCancelled = "cancelled" // 任务已取消
)

// 节点状态枚举
const (
	NodeStatusOnline   = "online"   // 节点在线
	NodeStatusOffline  = "offline"  // 节点离线
	NodeStatusBusy     = "busy"     // 节点忙碌
	NodeStatusStopping = "stopping" // 节点停止中
)

// ============================================================================
// 数据结构定义
// ============================================================================

// Task 表示一个工作任务
// 工作流控制中心将任务分配给节点，节点执行任务并更新状态
type Task struct {
	TaskID       string                 `json:"task_id"`       // 任务唯一标识
	NodeName     string                 `json:"node_name"`     // 目标节点名称
	ExecutionID  string                 `json:"execution_id"`  // 执行实例ID
	InstanceID   string                 `json:"instance_id"`   // 工作流实例ID
	WorkflowID   string                 `json:"workflow_id"`   // 工作流定义ID
	NodeID       string                 `json:"node_id"`       // 节点定义ID
	InputData    map[string]interface{} `json:"input_data"`    // 任务输入数据
	OutputData   map[string]interface{} `json:"output_data"`   // 任务输出数据
	Status       string                 `json:"status"`        // 任务状态
	Progress     float64                `json:"progress"`      // 任务进度 0-100
	ErrorMessage string                 `json:"error_message"` // 错误信息
	CreatedAt    int64                  `json:"created_at"`    // 创建时间戳(毫秒)
	StartedAt    int64                  `json:"started_at"`    // 开始执行时间戳(毫秒)
	CompletedAt  int64                  `json:"completed_at"`  // 完成时间戳(毫秒)
	RetryCount   int                    `json:"retry_count"`   // 已重试次数
}

// NodeInfo 表示工作节点信息
// 节点注册到工作流控制中心，用于接收任务
type NodeInfo struct {
	NodeName      string   `json:"node_name"`      // 节点唯一名称
	InstanceID    string   `json:"instance_id"`    // 实例唯一ID
	NodeType      string   `json:"node_type"`      // 节点类型
	Host          string   `json:"host"`           // 节点主机地址
	Port          int      `json:"port"`           // 节点服务端口
	Status        string   `json:"status"`         // 节点状态
	Capabilities  []string `json:"capabilities"`   // 节点能力列表
	CurrentLoad   int      `json:"current_load"`   // 当前负载(正在执行的任务数)
	MaxCapacity   int      `json:"max_capacity"`   // 最大容量(可同时执行的任务数)
	Version       string   `json:"version"`        // 节点版本号
	RegisteredAt  int64    `json:"registered_at"`  // 注册时间戳(毫秒)
	LastHeartbeat int64    `json:"last_heartbeat"` // 最后心跳时间戳(毫秒)
}

// WorkerConfig 工作节点配置结构
// 包含节点自身配置、Redis配置、TableStore配置、OSS配置和工作线程配置
type WorkerConfig struct {
	NodeName     string           `yaml:"node_name"`    // 节点唯一名称
	NodeType     string           `yaml:"node_type"`    // 节点类型
	Host         string           `yaml:"host"`         // 节点主机地址
	Port         int              `yaml:"port"`         // 节点服务端口
	Capabilities []string         `yaml:"capabilities"` // 节点能力列表
	MaxCapacity  int              `yaml:"max_capacity"` // 最大容量
	Redis        RedisConfig      `yaml:"redis"`        // Redis配置
	TableStore   TableStoreConfig `yaml:"tablestore"`   // TableStore配置
	OSS          OSSConfig        `yaml:"oss"`          // OSS配置
	Worker       WorkerSettings   `yaml:"worker"`       // 工作线程配置
}

// RedisConfig Redis连接配置
type RedisConfig struct {
	Addr     string `yaml:"addr"`     // Redis地址，格式: host:port
	Password string `yaml:"password"` // Redis密码
	DB       int    `yaml:"db"`       // Redis数据库编号
}

// TableStoreConfig 阿里云TableStore配置
type TableStoreConfig struct {
	Endpoint        string `yaml:"endpoint"`          // TableStore服务端点
	AccessKeyID     string `yaml:"access_key_id"`     // 访问密钥ID
	AccessKeySecret string `yaml:"access_key_secret"` // 访问密钥密
	InstanceName    string `yaml:"instance_name"`     // 实例名称
	TableName       string `yaml:"table_name"`        // 表名称
}

// OSSConfig 阿里云OSS配置
type OSSConfig struct {
	Endpoint        string `yaml:"endpoint"`          // OSS服务端点
	AccessKeyID     string `yaml:"access_key_id"`     // 访问密钥ID
	AccessKeySecret string `yaml:"access_key_secret"` // 访问密钥密码
	BucketName      string `yaml:"bucket_name"`       // 存储桶名称
}

// WorkerSettings 工作线程运行参数配置
type WorkerSettings struct {
	PollInterval      time.Duration `yaml:"poll_interval"`      // 任务拉取间隔(秒)
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"` // 心跳发送间隔(秒)
	MaxRetries        int           `yaml:"max_retries"`        // 最大重试次数
	BatchSize         int           `yaml:"batch_size"`         // 批量处理大小
}

// ============================================================================
// 构造函数和辅助方法
// ============================================================================

// NewTask 创建一个新的任务对象
// 初始化任务为等待状态，设置创建时间
// 返回: 初始化的Task指针
func NewTask() *Task {
	return &Task{
		Status:    TaskStatusPending,      // 初始状态为等待中
		Progress:  0,                      // 初始进度为0
		CreatedAt: time.Now().UnixMilli(), // 设置创建时间
	}
}

// IsCompleted 判断任务是否已完成
// 完成任务包括: 成功、失败、取消三种状态
// 返回: true表示任务已完成
func (t *Task) IsCompleted() bool {
	return t.Status == TaskStatusCompleted || t.Status == TaskStatusFailed || t.Status == TaskStatusCancelled
}

// MarkRunning 标记任务为执行中状态
// 更新任务状态为running，并记录开始时间
func (t *Task) MarkRunning() {
	t.Status = TaskStatusRunning         // 设置状态为执行中
	t.StartedAt = time.Now().UnixMilli() // 记录开始时间
}

// MarkCompleted 标记任务为完成状态
// 更新任务状态为completed，设置输出数据，进度设为100%，记录完成时间
// 参数: output 任务执行结果数据
func (t *Task) MarkCompleted(output map[string]interface{}) {
	t.Status = TaskStatusCompleted         // 设置状态为已完成
	t.OutputData = output                  // 保存输出数据
	t.Progress = 100                       // 设置进度为100%
	t.CompletedAt = time.Now().UnixMilli() // 记录完成时间
}

// MarkFailed 标记任务为失败状态
// 更新任务状态为failed，记录错误信息，完成时间
// 参数: err 错误信息描述
func (t *Task) MarkFailed(err string) {
	t.Status = TaskStatusFailed            // 设置状态为失败
	t.ErrorMessage = err                   // 保存错误信息
	t.CompletedAt = time.Now().UnixMilli() // 记录完成时间
}

// UpdateProgress 更新任务进度
// 用于任务执行过程中更新进度和中间结果
// 参数:
//   - progress: 当前进度(0-100)
//   - output: 进度更新时的输出数据(可选，传nil表示不更新)
func (t *Task) UpdateProgress(progress float64, output map[string]interface{}) {
	t.Progress = progress // 更新进度值
	if output != nil {
		t.OutputData = output // 更新输出数据
	}
}

// NewNodeInfo 创建一个新的节点信息对象
// 初始化节点为在线状态，设置注册时间和心跳时间
// 参数:
//   - name: 节点名称
//   - nodeType: 节点类型
//   - host: 主机地址
//   - port: 端口号
//   - capabilities: 节点能力列表
//   - maxCapacity: 最大容量
//   - version: 版本号
//
// 返回: 初始化的NodeInfo指针
func NewNodeInfo(name, nodeType, host string, port int, capabilities []string, maxCapacity int, version string) *NodeInfo {
	instanceID := generateInstanceID()
	return &NodeInfo{
		NodeName:      name,                   // 节点名称
		InstanceID:    instanceID,             // 实例唯一ID
		NodeType:      nodeType,               // 节点类型
		Host:          host,                   // 主机地址
		Port:          port,                   // 端口号
		Status:        NodeStatusOnline,       // 初始状态为在线
		Capabilities:  capabilities,           // 能力列表
		CurrentLoad:   0,                      // 初始负载为0
		MaxCapacity:   maxCapacity,            // 最大容量
		Version:       version,                // 版本号
		RegisteredAt:  time.Now().UnixMilli(), // 注册时间
		LastHeartbeat: time.Now().UnixMilli(), // 最后心跳时间
	}
}
