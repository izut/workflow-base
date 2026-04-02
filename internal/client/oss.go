package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
)

// ============================================================================
// OSS客户端结构体定义
// ============================================================================

// OSSClient 阿里云OSS客户端封装
// 用于文件上传、下载、管理等功能
type OSSClient struct {
	client     *oss.Client // 底层OSS客户端
	bucketName string      // 存储桶名称
	region     string      // 区域
	endpoint   string      // 端点
}

// ============================================================================
// 构造函数
// ============================================================================

// NewOSSClient 创建OSS客户端
// 参数:
//   - endpoint: OSS服务端点，格式: https://oss-cn-hangzhou.aliyuncs.com
//   - accessKeyID: 访问密钥ID
//   - accessKeySecret: 访问密钥密码
//   - bucketName: 存储桶名称
//
// 返回:
//   - *OSSClient: OSS客户端指针
//   - error: 初始化失败时返回错误
func NewOSSClient(endpoint, accessKeyID, accessKeySecret, bucketName string) (*OSSClient, error) {
	region := extractRegionFromEndpoint(endpoint)

	credProvider := credentials.NewStaticCredentialsProvider(accessKeyID, accessKeySecret, "")

	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credProvider).
		WithRegion(region).
		WithEndpoint(endpoint)

	client := oss.NewClient(cfg)

	return &OSSClient{
		client:     client,
		bucketName: bucketName,
		region:     region,
		endpoint:   endpoint,
	}, nil
}

// extractRegionFromEndpoint 从endpoint中提取region
// endpoint格式: https://oss-cn-hangzhou.aliyuncs.com
func extractRegionFromEndpoint(endpoint string) string {
	endpoint = removePrefix(endpoint, "https://")
	endpoint = removePrefix(endpoint, "http://")
	endpoint = removeSuffix(endpoint, ".aliyuncs.com")
	endpoint = removePrefix(endpoint, "oss-")
	return endpoint
}

func removePrefix(s, prefix string) string {
	if len(s) >= len(prefix) && s[:len(prefix)] == prefix {
		return s[len(prefix):]
	}
	return s
}

func removeSuffix(s, suffix string) string {
	if len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix {
		return s[:len(s)-len(suffix)]
	}
	return s
}

// ============================================================================
// 文件上传操作
// ============================================================================

// UploadFile 上传本地文件到OSS
// 参数:
//   - objectKey: OSS中的对象键（路径）
//   - filePath: 本地文件路径
//   - options: 可选配置
//
// 返回:
//   - string: 文件的OSS URL
//   - error: 上传失败时返回错误
func (o *OSSClient) UploadFile(objectKey, filePath string, options ...Option) (string, error) {
	if _, err := os.Stat(filePath); err != nil {
		return "", fmt.Errorf("file not found: %w", err)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	url, err := o.UploadReader(objectKey, file, "", -1, options...)
	if err != nil {
		return "", err
	}

	return url, nil
}

// UploadData 上传字节数据到OSS
// 参数:
//   - objectKey: OSS中的对象键（路径）
//   - data: 要上传的字节数据
//   - contentType: 内容类型（如 "application/json"）
//   - options: 可选配置
//
// 返回:
//   - string: 文件的OSS URL
//   - error: 上传失败时返回错误
func (o *OSSClient) UploadData(objectKey string, data []byte, contentType string, options ...Option) (string, error) {
	reader := bytes.NewReader(data)
	return o.UploadReader(objectKey, reader, contentType, int64(len(data)), options...)
}

// UploadReader 上传Reader到OSS
// 参数:
//   - objectKey: OSS中的对象键（路径）
//   - reader: 数据读取器
//   - contentType: 内容类型
//   - size: 数据大小（-1表示未知）
//   - options: 可选配置
//
// 返回:
//   - string: 文件的OSS URL
//   - error: 上传失败时返回错误
func (o *OSSClient) UploadReader(objectKey string, reader io.Reader, contentType string, size int64, options ...Option) (string, error) {
	request := &oss.PutObjectRequest{
		Bucket: oss.Ptr(o.bucketName),
		Key:    oss.Ptr(objectKey),
		Body:   reader,
	}

	if contentType != "" {
		request.ContentType = oss.Ptr(contentType)
	}

	opts := &uploadOptions{}
	for _, opt := range options {
		opt.apply(opts)
	}

	if opts.contentType != "" {
		request.ContentType = oss.Ptr(opts.contentType)
	}
	if opts.metadata != nil {
		request.Metadata = opts.metadata
	}

	_, err := o.client.PutObject(context.Background(), request)
	if err != nil {
		return "", fmt.Errorf("failed to upload object: %w", err)
	}

	return o.getObjectURL(objectKey), nil
}

// ============================================================================
// 文件下载操作
// ============================================================================

// DownloadFile 从OSS下载文件到本地
// 参数:
//   - objectKey: OSS中的对象键
//   - filePath: 本地保存路径
//   - options: 可选配置
//
// 返回:
//   - error: 下载失败时返回错误
func (o *OSSClient) DownloadFile(objectKey, filePath string, options ...Option) error {
	data, err := o.DownloadData(objectKey, options...)
	if err != nil {
		return err
	}

	return os.WriteFile(filePath, data, 0644)
}

// DownloadData 从OSS下载数据到内存
// 参数:
//   - objectKey: OSS中的对象键
//   - options: 可选配置
//
// 返回:
//   - []byte: 下载的数据
//   - error: 下载失败时返回错误
func (o *OSSClient) DownloadData(objectKey string, options ...Option) ([]byte, error) {
	request := &oss.GetObjectRequest{
		Bucket: oss.Ptr(o.bucketName),
		Key:    oss.Ptr(objectKey),
	}

	response, err := o.client.GetObject(context.Background(), request)
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	defer response.Body.Close()

	return io.ReadAll(response.Body)
}

// ============================================================================
// 文件管理操作
// ============================================================================

// DeleteFile 删除OSS中的文件
// 参数:
//   - objectKey: OSS中的对象键
//
// 返回:
//   - error: 删除失败时返回错误
func (o *OSSClient) DeleteFile(objectKey string) error {
	request := &oss.DeleteObjectRequest{
		Bucket: oss.Ptr(o.bucketName),
		Key:    oss.Ptr(objectKey),
	}

	_, err := o.client.DeleteObject(context.Background(), request)
	if err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}
	return nil
}

// DeleteFiles 批量删除OSS中的文件
// 参数:
//   - objectKeys: 对象键列表
//
// 返回:
//   - error: 删除失败时返回错误
func (o *OSSClient) DeleteFiles(objectKeys []string) error {
	var deleteObjects []oss.DeleteObject
	for _, key := range objectKeys {
		deleteObjects = append(deleteObjects, oss.DeleteObject{Key: oss.Ptr(key)})
	}

	request := &oss.DeleteMultipleObjectsRequest{
		Bucket: oss.Ptr(o.bucketName),
		Delete: &oss.Delete{
			Objects: deleteObjects,
		},
	}

	_, err := o.client.DeleteMultipleObjects(context.Background(), request)
	if err != nil {
		return fmt.Errorf("failed to delete objects: %w", err)
	}
	return nil
}

// FileExists 检查文件是否存在
// 参数:
//   - objectKey: OSS中的对象键
//
// 返回:
//   - bool: 文件是否存在
//   - error: 查询失败时返回错误
func (o *OSSClient) FileExists(objectKey string) (bool, error) {
	request := &oss.HeadObjectRequest{
		Bucket: oss.Ptr(o.bucketName),
		Key:    oss.Ptr(objectKey),
	}

	_, err := o.client.HeadObject(context.Background(), request)
	if err != nil {
		return false, nil
	}
	return true, nil
}

// GetFileURL 获取文件的公网URL（注意：不支持签名URL时返回普通URL）
// 参数:
//   - objectKey: OSS中的对象键
//   - expiry: URL过期时间（秒）（暂不支持）
//
// 返回:
//   - string: 文件URL
//   - error: 始终返回nil
func (o *OSSClient) GetFileURL(objectKey string, expiry int64) (string, error) {
	return o.getObjectURL(objectKey), nil
}

// ============================================================================
// 文件列表操作
// ============================================================================

// ListFiles 列出OSS中的文件
// 参数:
//   - prefix: 前缀过滤
//   - maxKeys: 最大返回数量
//   - marker: 分页标记
//
// 返回:
//   - []string: 文件对象键列表
//   - string: 下次分页标记
//   - error: 查询失败时返回错误
func (o *OSSClient) ListFiles(prefix string, maxKeys int, marker string) ([]string, string, error) {
	request := &oss.ListObjectsV2Request{
		Bucket:     oss.Ptr(o.bucketName),
		Prefix:     oss.Ptr(prefix),
		StartAfter: oss.Ptr(marker),
	}

	response, err := o.client.ListObjectsV2(context.Background(), request)
	if err != nil {
		return nil, "", fmt.Errorf("failed to list objects: %w", err)
	}

	var keys []string
	for _, obj := range response.Contents {
		keys = append(keys, oss.ToString(obj.Key))
	}

	var nextMarker string
	if response.NextContinuationToken != nil {
		nextMarker = oss.ToString(response.NextContinuationToken)
	}

	return keys, nextMarker, nil
}

// GetFileInfo 获取文件信息
// 参数:
//   - objectKey: OSS中的对象键
//
// 返回:
//   - *oss.HeadObjectResult: 文件属性
//   - error: 查询失败时返回错误
func (o *OSSClient) GetFileInfo(objectKey string) (*oss.HeadObjectResult, error) {
	request := &oss.HeadObjectRequest{
		Bucket: oss.Ptr(o.bucketName),
		Key:    oss.Ptr(objectKey),
	}

	return o.client.HeadObject(context.Background(), request)
}

// ============================================================================
// 辅助方法
// ============================================================================

// getObjectURL 获取对象的公网URL
func (o *OSSClient) getObjectURL(objectKey string) string {
	return fmt.Sprintf("https://%s.%s.aliyuncs.com/%s", o.bucketName, o.region, objectKey)
}

// ============================================================================
// 选项模式
// ============================================================================

// uploadOptions 上传可选配置
type uploadOptions struct {
	contentType string
	metadata    map[string]string
}

// Option OSS选项函数类型
type Option interface {
	apply(opts *uploadOptions)
}

// optionFunc 函数式选项实现
type optionFunc func(opts *uploadOptions)

func (f optionFunc) apply(opts *uploadOptions) {
	f(opts)
}

// WithContentType 设置内容类型
func WithContentType(contentType string) Option {
	return optionFunc(func(opts *uploadOptions) {
		opts.contentType = contentType
	})
}

// WithMetadata 设置元数据
func WithMetadata(key, value string) Option {
	return optionFunc(func(opts *uploadOptions) {
		if opts.metadata == nil {
			opts.metadata = make(map[string]string)
		}
		opts.metadata[key] = value
	})
}
