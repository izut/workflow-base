package config

import (
	"fmt"
	"os"
	"time"

	"github.com/izut/workflow-base/internal/model"
	"gopkg.in/yaml.v3"
)

// ============================================================================
// 配置加载器结构体定义
// ============================================================================

// ConfigLoader 配置加载器
// 支持从YAML文件和 环境变量加载配置
type ConfigLoader struct{}

// ============================================================================
// 构造函数
// ============================================================================

// NewConfigLoader 创建配置加载器实例
//
// 返回: ConfigLoader指针
func NewConfigLoader() *ConfigLoader {
	return &ConfigLoader{}
}

// ============================================================================
// 配置文件加载
// ============================================================================

// LoadFromFile 从YAML文件加载配置
// 参数:
//   - path: 配置文件路径
//
// 返回:
//   - *model.WorkerConfig: 解析后的配置对象
//   - error: 加载或解析失败时返回错误
func (c *ConfigLoader) LoadFromFile(path string) (*model.WorkerConfig, error) {
	// 读取配置文件内容
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// 解析配置内容
	return c.LoadFromBytes(data)
}

// LoadFromBytes 从字节数据加载配置
// 参数:
//   - data: YAML格式的配置数据
//
// 返回:
//   - *model.WorkerConfig: 解析后的配置对象
//   - error: 解析失败时返回错误
func (c *ConfigLoader) LoadFromBytes(data []byte) (*model.WorkerConfig, error) {
	// 解析YAML数据
	var config model.WorkerConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// 验证配置有效性
	if err := c.validate(&config); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &config, nil
}

// ============================================================================
// 环境变量加载
// ============================================================================

// LoadFromEnv 从环境变量加载配置
// 环境变量优先级高于默认配置
//
// 返回:
//   - *model.WorkerConfig: 解析后的配置对象
//   - error: 配置无效时返回错误
func (c *ConfigLoader) LoadFromEnv() (*model.WorkerConfig, error) {
	// 构建配置对象，从环境变量读取值
	config := &model.WorkerConfig{
		// 节点基础配置
		NodeName:     getEnvOrDefault("WORKER_NODE_NAME", "worker-default"), // 节点名称
		NodeType:     getEnvOrDefault("WORKER_NODE_TYPE", "generic"),        // 节点类型
		Host:         getEnvOrDefault("WORKER_HOST", "localhost"),           // 主机地址
		Port:         getEnvIntOrDefault("WORKER_PORT", 8080),               // 服务端口
		Capabilities: []string{},                                            // 节点能力列表
		MaxCapacity:  getEnvIntOrDefault("WORKER_MAX_CAPACITY", 10),         // 最大容量

		// Redis配置
		Redis: model.RedisConfig{
			Addr:     getEnvOrDefault("REDIS_ADDR", "localhost:6379"), // Redis地址
			Password: getEnvOrDefault("REDIS_PASSWORD", ""),           // Redis密码
			DB:       getEnvIntOrDefault("REDIS_DB", 0),               // Redis数据库编号
		},

		// TableStore配置
		TableStore: model.TableStoreConfig{
			Endpoint:        getEnvOrDefault("TABLESTORE_ENDPOINT", ""),              // TableStore端点
			AccessKeyID:     getEnvOrDefault("TABLESTORE_ACCESS_KEY_ID", ""),         // AccessKey ID
			AccessKeySecret: getEnvOrDefault("TABLESTORE_ACCESS_KEY_SECRET", ""),     // AccessKey Secret
			InstanceName:    getEnvOrDefault("TABLESTORE_INSTANCE_NAME", ""),         // 实例名称
			TableName:       getEnvOrDefault("TABLESTORE_TABLE_NAME", "worker_node"), // 表名称
		},

		// OSS配置
		OSS: model.OSSConfig{
			Endpoint:        getEnvOrDefault("OSS_ENDPOINT", ""),          // OSS端点
			AccessKeyID:     getEnvOrDefault("OSS_ACCESS_KEY_ID", ""),     // AccessKey ID
			AccessKeySecret: getEnvOrDefault("OSS_ACCESS_KEY_SECRET", ""), // AccessKey Secret
			BucketName:      getEnvOrDefault("OSS_BUCKET_NAME", ""),       // 存储桶名称
		},

		// 工作线程配置
		Worker: model.WorkerSettings{
			PollInterval:      time.Duration(getEnvDurationOrDefault("WORKER_POLL_INTERVAL", 5)),       // 任务拉取间隔(秒)
			HeartbeatInterval: time.Duration(getEnvDurationOrDefault("WORKER_HEARTBEAT_INTERVAL", 30)), // 心跳间隔(秒)
			MaxRetries:        getEnvIntOrDefault("WORKER_MAX_RETRIES", 3),                             // 最大重试次数
			BatchSize:         getEnvIntOrDefault("WORKER_BATCH_SIZE", 1),                              // 批量处理大小
		},
	}

	// 验证配置有效性
	if err := c.validate(config); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return config, nil
}

// ============================================================================
// 配置验证
// ============================================================================

// validate 验证配置有效性
// 检查必填字段是否已配置
// 参数:
//   - config: 要验证的配置对象
//
// 返回: 配置无效时返回错误
func (c *ConfigLoader) validate(config *model.WorkerConfig) error {
	// 检查节点名称是否配置
	if config.NodeName == "" {
		return fmt.Errorf("node_name is required")
	}
	// 检查Redis地址是否配置
	if config.Redis.Addr == "" {
		return fmt.Errorf("redis.addr is required")
	}
	return nil
}

// ============================================================================
// 环境变量辅助函数
// ============================================================================

// getEnvOrDefault 获取环境变量值，如果未设置则返回默认值
// 参数:
//   - key: 环境变量名称
//   - defaultValue: 默认值
//
// 返回: 环境变量值或默认值
func getEnvOrDefault(key, defaultValue string) string {
	// 尝试获取环境变量
	if value := os.Getenv(key); value != "" {
		return value
	}
	// 返回默认值
	return defaultValue
}

// getEnvIntOrDefault 获取整型环境变量值，如果未设置则返回默认值
// 参数:
//   - key: 环境变量名称
//   - defaultValue: 默认值
//
// 返回: 环境变量值或默认值
func getEnvIntOrDefault(key string, defaultValue int) int {
	// 尝试获取环境变量
	if value := os.Getenv(key); value != "" {
		var intValue int
		// 尝试解析为整数
		if _, err := fmt.Sscanf(value, "%d", &intValue); err == nil {
			return intValue
		}
	}
	// 返回默认值
	return defaultValue
}

// getEnvDurationOrDefault 获取时长环境变量值(秒)，如果未设置则返回默认值
// 参数:
//   - key: 环境变量名称
//   - defaultValue: 默认值(秒)
//
// 返回: 环境变量值或默认值
func getEnvDurationOrDefault(key string, defaultValue int) int {
	// 尝试获取环境变量
	if value := os.Getenv(key); value != "" {
		var intValue int
		// 尝试解析为整数
		if _, err := fmt.Sscanf(value, "%d", &intValue); err == nil {
			return intValue
		}
	}
	// 返回默认值
	return defaultValue
}
