package client

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/izut/workflow-base/internal/model"
)

// ============================================================================
// TableStore客户端结构体定义
// ============================================================================

// TableStoreClient 阿里云TableStore客户端封装
// 用于持久化存储节点注册信息
type TableStoreClient struct {
	client    *tablestore.TableStoreClient // 底层TableStore客户端
	tableName string                       // 表名称
}

// ============================================================================
// 构造函数
// ============================================================================

// NewTableStoreClient 创建TableStore客户端
// 参数:
//   - endpoint: TableStore服务端点，格式: https://instance.cn-hangzhou.ots.aliyuncs.com
//   - accessKeyID: 访问密钥ID
//   - accessKeySecret: 访问密钥密码
//   - instanceName: TableStore实例名称
//   - tableName: 表名称
//
// 返回:
//   - *TableStoreClient: TableStore客户端指针
//   - error: 初始化失败时返回错误
func NewTableStoreClient(endpoint, accessKeyID, accessKeySecret, instanceName, tableName string) (*TableStoreClient, error) {
	// 创建TableStore客户端
	client := tablestore.NewClient(endpoint, instanceName, accessKeyID, accessKeySecret)

	return &TableStoreClient{
		client:    client,
		tableName: tableName,
	}, nil
}

// ============================================================================
// 节点注册操作
// ============================================================================

// RegisterNode 向TableStore注册节点信息
// 将节点信息写入TableStore，用于持久化存储节点元数据
// 参数:
//   - nodeInfo: 节点信息对象
//
// 返回: 操作失败时返回错误
func (t *TableStoreClient) RegisterNode(nodeInfo *model.NodeInfo) error {
	// 构建写入请求
	putRowChange := &tablestore.PutRowChange{
		TableName: t.tableName,
	}

	// 添加主键：使用节点名称作为主键，保证唯一性
	putRowChange.PrimaryKey = &tablestore.PrimaryKey{}
	putRowChange.PrimaryKey.AddPrimaryKeyColumn("node_name", nodeInfo.NodeName)

	// 添加属性列：存储节点详细信息
	putRowChange.AddColumn("node_type", nodeInfo.NodeType)
	putRowChange.AddColumn("host", nodeInfo.Host)
	putRowChange.AddColumn("port", strconv.Itoa(nodeInfo.Port))
	putRowChange.AddColumn("status", nodeInfo.Status)
	putRowChange.AddColumn("capabilities", fmt.Sprintf("%v", nodeInfo.Capabilities))
	putRowChange.AddColumn("current_load", strconv.Itoa(nodeInfo.CurrentLoad))
	putRowChange.AddColumn("max_capacity", strconv.Itoa(nodeInfo.MaxCapacity))
	putRowChange.AddColumn("version", nodeInfo.Version)
	putRowChange.AddColumn("registered_at", strconv.FormatInt(nodeInfo.RegisteredAt, 10))
	putRowChange.AddColumn("heartbeat_at", strconv.FormatInt(nodeInfo.LastHeartbeat, 10))

	// 设置行条件：期望行不存在（用于插入新行）
	putRowChange.SetCondition(tablestore.RowExistenceExpectation_EXPECT_NOT_EXIST)

	// 执行写入操作
	putRowRequest := &tablestore.PutRowRequest{
		PutRowChange: putRowChange,
	}
	_, err := t.client.PutRow(putRowRequest)
	if err != nil {
		return fmt.Errorf("failed to register node to tablestore: %w", err)
	}

	return nil
}

// ============================================================================
// 心跳更新操作
// ============================================================================

// UpdateNodeHeartbeat 更新节点心跳信息
// 定期更新节点状态和最后心跳时间
// 参数:
//   - nodeInfo: 节点信息对象
//
// 返回: 操作失败时返回错误
func (t *TableStoreClient) UpdateNodeHeartbeat(nodeInfo *model.NodeInfo) error {
	// 构建更新请求
	updateRowChange := &tablestore.UpdateRowChange{
		TableName: t.tableName,
	}

	// 设置主键
	updateRowChange.PrimaryKey = &tablestore.PrimaryKey{}
	updateRowChange.PrimaryKey.AddPrimaryKeyColumn("node_name", nodeInfo.NodeName)

	// 添加更新列（使用PutColumn而不是AddColumn）
	updateRowChange.PutColumn("status", nodeInfo.Status)
	updateRowChange.PutColumn("current_load", strconv.Itoa(nodeInfo.CurrentLoad))
	updateRowChange.PutColumn("heartbeat_at", strconv.FormatInt(nodeInfo.LastHeartbeat, 10))

	// 设置行条件：期望行存在
	updateRowChange.SetCondition(tablestore.RowExistenceExpectation_EXPECT_EXIST)

	// 执行更新操作
	updateRowRequest := &tablestore.UpdateRowRequest{
		UpdateRowChange: updateRowChange,
	}
	_, err := t.client.UpdateRow(updateRowRequest)
	if err != nil {
		return fmt.Errorf("failed to update node heartbeat: %w", err)
	}

	return nil
}

// ============================================================================
// 节点信息查询
// ============================================================================

// GetNodeInfo 根据节点名称查询节点信息
// 参数:
//   - nodeName: 节点名称
//
// 返回:
//   - *model.NodeInfo: 节点信息指针
//   - error: 查询失败或节点不存在时返回错误
func (t *TableStoreClient) GetNodeInfo(nodeName string) (*model.NodeInfo, error) {
	// 构建查询请求
	criteria := &tablestore.SingleRowQueryCriteria{
		TableName: t.tableName,
	}

	// 设置主键
	criteria.PrimaryKey = &tablestore.PrimaryKey{}
	criteria.PrimaryKey.AddPrimaryKeyColumn("node_name", nodeName)

	getRowRequest := &tablestore.GetRowRequest{
		SingleRowQueryCriteria: criteria,
	}

	// 执行查询
	resp, err := t.client.GetRow(getRowRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to get node info: %w", err)
	}

	// 检查是否找到记录
	if len(resp.PrimaryKey.PrimaryKeys) == 0 {
		return nil, fmt.Errorf("node not found: %s", nodeName)
	}

	// 解析返回结果
	nodeInfo := &model.NodeInfo{
		NodeName: nodeName,
	}

	// 遍历所有属性列，填充节点信息
	for _, col := range resp.Columns {
		switch col.ColumnName {
		case "node_type":
			nodeInfo.NodeType = col.Value.(string)
		case "host":
			nodeInfo.Host = col.Value.(string)
		case "port":
			nodeInfo.Port, _ = strconv.Atoi(col.Value.(string))
		case "status":
			nodeInfo.Status = col.Value.(string)
		case "capabilities":
			nodeInfo.Capabilities = parseSlice(col.Value.(string))
		case "current_load":
			nodeInfo.CurrentLoad, _ = strconv.Atoi(col.Value.(string))
		case "max_capacity":
			nodeInfo.MaxCapacity, _ = strconv.Atoi(col.Value.(string))
		case "version":
			nodeInfo.Version = col.Value.(string)
		case "registered_at":
			nodeInfo.RegisteredAt, _ = strconv.ParseInt(col.Value.(string), 10, 64)
		case "heartbeat_at":
			nodeInfo.LastHeartbeat, _ = strconv.ParseInt(col.Value.(string), 10, 64)
		}
	}

	return nodeInfo, nil
}

// ============================================================================
// 节点注销操作
// ============================================================================

// UnregisterNode 从TableStore中删除节点信息
// 参数:
//   - nodeName: 节点名称
//
// 返回: 操作失败时返回错误
func (t *TableStoreClient) UnregisterNode(nodeName string) error {
	// 构建删除请求
	deleteRowChange := &tablestore.DeleteRowChange{
		TableName: t.tableName,
	}

	// 设置主键
	deleteRowChange.PrimaryKey = &tablestore.PrimaryKey{}
	deleteRowChange.PrimaryKey.AddPrimaryKeyColumn("node_name", nodeName)

	// 设置行条件：期望行存在
	deleteRowChange.SetCondition(tablestore.RowExistenceExpectation_EXPECT_EXIST)

	// 执行删除操作
	deleteRowRequest := &tablestore.DeleteRowRequest{
		DeleteRowChange: deleteRowChange,
	}
	_, err := t.client.DeleteRow(deleteRowRequest)
	if err != nil {
		return fmt.Errorf("failed to unregister node: %w", err)
	}

	return nil
}

// ============================================================================
// 节点列表查询
// ============================================================================

// ListNodes 获取所有已注册的节点列表
// 使用范围查询扫描全表
//
// 返回:
//   - []*model.NodeInfo: 节点信息切片
//   - error: 查询失败时返回错误
func (t *TableStoreClient) ListNodes() ([]*model.NodeInfo, error) {
	// 构建范围查询请求
	rangeRowQueryCriteria := &tablestore.RangeRowQueryCriteria{
		TableName: t.tableName,
		Direction: tablestore.FORWARD, // 正序扫描
	}

	// 起始主键：空字符串表示从第一条开始
	rangeRowQueryCriteria.StartPrimaryKey = &tablestore.PrimaryKey{}
	rangeRowQueryCriteria.StartPrimaryKey.AddPrimaryKeyColumn("node_name", "")

	// 结束主键：\xFF表示到最后一个主键
	rangeRowQueryCriteria.EndPrimaryKey = &tablestore.PrimaryKey{}
	rangeRowQueryCriteria.EndPrimaryKey.AddPrimaryKeyColumn("node_name", string([]byte{255}))

	getRangeRequest := &tablestore.GetRangeRequest{
		RangeRowQueryCriteria: rangeRowQueryCriteria,
	}

	// 执行范围查询
	resp, err := t.client.GetRange(getRangeRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to list nodes: %w", err)
	}

	// 解析返回的所有行
	var nodes []*model.NodeInfo
	for _, row := range resp.Rows {
		nodeInfo := &model.NodeInfo{}

		// 解析主键
		for _, col := range row.PrimaryKey.PrimaryKeys {
			if col.ColumnName == "node_name" {
				nodeInfo.NodeName = col.Value.(string)
			}
		}

		// 解析属性列
		for _, col := range row.Columns {
			switch col.ColumnName {
			case "node_type":
				nodeInfo.NodeType = col.Value.(string)
			case "host":
				nodeInfo.Host = col.Value.(string)
			case "port":
				nodeInfo.Port, _ = strconv.Atoi(col.Value.(string))
			case "status":
				nodeInfo.Status = col.Value.(string)
			case "capabilities":
				nodeInfo.Capabilities = parseSlice(col.Value.(string))
			case "current_load":
				nodeInfo.CurrentLoad, _ = strconv.Atoi(col.Value.(string))
			case "max_capacity":
				nodeInfo.MaxCapacity, _ = strconv.Atoi(col.Value.(string))
			case "version":
				nodeInfo.Version = col.Value.(string)
			case "registered_at":
				nodeInfo.RegisteredAt, _ = strconv.ParseInt(col.Value.(string), 10, 64)
			case "heartbeat_at":
				nodeInfo.LastHeartbeat, _ = strconv.ParseInt(col.Value.(string), 10, 64)
			}
		}
		nodes = append(nodes, nodeInfo)
	}

	return nodes, nil
}

// ============================================================================
// 辅助函数
// ============================================================================

// parseSlice 解析序列化的字符串切片
// 将 "[item1, item2, item3]" 格式转换为 []string
// 参数:
//   - s: 序列化的字符串
//
// 返回: 解析后的字符串切片
func parseSlice(s string) []string {
	// 处理空值情况
	if s == "" || s == "[]" {
		return nil
	}
	// 去掉首尾方括号
	s = s[1 : len(s)-1]
	if s == "" {
		return nil
	}
	// 按逗号分割并去除首尾空格和引号
	var result []string
	for _, item := range strings.Split(s, ",") {
		result = append(result, strings.Trim(item, " \""))
	}
	return result
}
