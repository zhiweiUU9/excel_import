package main

import (
	"bytes"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"flag"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	_ "github.com/go-sql-driver/mysql"
	"github.com/xuri/excelize/v2"
)

// 添加配置结构体
type Config struct {
	DSN          string
	OSS          OSSConfig
	DBTable      string
	DBFields     []string
	FilePath     string
	CateID       string
	OssUploadDir string
}

type OSSConfig struct {
	Endpoint        string
	AccessKeyID     string
	AccessKeySecret string
	BucketName      string
}

// 列表头映射
type ColumnMap struct {
	Name        int
	Branchname  int
	Province    int
	City        int
	Regionname  int
	Address     int
	Longitude   int
	Latitude    int
	Default_pic int
}

// 限流器
type RateLimiter struct {
	tokens  chan struct{}
	timeout time.Duration
}

// 创建限流器
func NewRateLimiter(rate int, timeout time.Duration) *RateLimiter {
	rl := &RateLimiter{
		tokens:  make(chan struct{}, rate),
		timeout: timeout,
	}
	// 初始化令牌
	for i := 0; i < rate; i++ {
		rl.tokens <- struct{}{}
	}
	return rl
}

// 获取令牌
func (rl *RateLimiter) acquire() {
	<-rl.tokens
	// 延迟释放令牌
	go func() {
		time.Sleep(rl.timeout)
		rl.tokens <- struct{}{}
	}()
}

// 添加函数来创建列映射
func createColumnMap(headers []string) (*ColumnMap, error) {
	cm := &ColumnMap{
		Name:        -1,
		Branchname:  -1,
		Province:    -1,
		City:        -1,
		Regionname:  -1,
		Address:     -1,
		Longitude:   -1,
		Latitude:    -1,
		Default_pic: -1,
	}

	// 遍历表头，匹配列名
	for i, header := range headers {
		switch strings.TrimSpace(strings.ToLower(header)) {
		case "名称", "name":
			cm.Name = i
		case "简称", "branchname":
			cm.Branchname = i
		case "省份", "province":
			cm.Province = i
		case "城市", "city":
			cm.City = i
		case "区域", "regionname":
			cm.Regionname = i
		case "详细地址", "address":
			cm.Address = i
		case "经度", "longitude":
			cm.Longitude = i
		case "纬度", "latitude":
			cm.Latitude = i
		case "图片", "default_pic", "图片地址":
			cm.Default_pic = i
		}
	}

	// 验证必需的列是否存在
	if cm.Name == -1 {
		return nil, fmt.Errorf("未找到名称列")
	}
	if cm.Longitude == -1 || cm.Latitude == -1 {
		return nil, fmt.Errorf("未找到经纬度列")
	}
	if cm.Default_pic == -1 {
		return nil, fmt.Errorf("未找到图片地址列")
	}

	return cm, nil
}

// 添加断点续传相关的结构体
type ProcessState struct {
	LastProcessedRow int    `json:"last_processed_row"`
	FilePath         string `json:"file_path"`
}

// 添加保存和读取状态的函数
func saveState(state ProcessState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return os.WriteFile(".process_state", data, 0644)
}

func loadState() (*ProcessState, error) {
	data, err := os.ReadFile(".process_state")
	if err != nil {
		if os.IsNotExist(err) {
			return &ProcessState{LastProcessedRow: 0}, nil
		}
		return nil, err
	}

	var state ProcessState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

// 日志辅助函数
func logInfo(format string, args ...interface{}) {
	log.Printf("[INFO] "+format, args...)
}

func main() {
	var (
		filePath     = flag.String("file", "", "Excel文件路径 (必填)")
		tableName    = flag.String("table", "", "数据库表名 (必填)")
		cateID       = flag.String("cate", "", "分类ID (必填)")
		ossUploadDir = flag.String("oss_dir", "", "上传到OSS的文件路径 (必填)")
	)

	// 自定义 usage 信息
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "用法: %s -file <excel_file_path> -table <table_name> -cate <category_id> -oss_dir <oss_upload_dir>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\n参数说明:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	// 验证必填参数
	if *filePath == "" || *tableName == "" || *cateID == "" || *ossUploadDir == "" {
		fmt.Println("错误: 所有参数都是必填的")
		flag.Usage()
		os.Exit(1)
	}

	// 验证文件是否存在
	if _, err := os.Stat(*filePath); os.IsNotExist(err) {
		log.Fatalf("文件不存在: %s", *filePath)
	}

	config := Config{
		DSN: "xxx:xxx@tcp(127.0.0.1:3306)/db_name?charset=utf8mb4&parseTime=True&loc=Local",
		OSS: OSSConfig{
			Endpoint: "https://oss-cn-beijing.aliyuncs.com",
			//AccessKeyID:     "xxx",
			//AccessKeySecret: "xxx",
			//BucketName:      "xxx",
		},
		DBTable: *tableName,
		DBFields: []string{"name", "type", "cate_id", "address", "detailed_address", "image", "latitude",
			"longitude", "day_time", "day_start", "day_end", "add_time",
			"is_show", "is_del", "is_store", "default_delivery",
			"product_verify_status", "agent_type", "is_virtual"},
		FilePath:     *filePath,
		CateID:       *cateID,
		OssUploadDir: *ossUploadDir,
	}

	if err := run(config); err != nil {
		log.Fatalf("程序运行失败: %v", err)
	}
}

func run(config Config) error {
	db, err := sql.Open("mysql", config.DSN)
	if err != nil {
		return fmt.Errorf("数据库连接失败: %v", err)
	}
	defer db.Close()

	return processExcelFile(config.FilePath, db, config, 500)
}

func processExcelFile(filePath string, db *sql.DB, config Config, chunkSize int) error {
	logInfo("开始处理文件: %s", filePath)

	f, err := excelize.OpenFile(filePath)
	if err != nil {
		return fmt.Errorf("无法打开文件: %v", err)
	}
	defer f.Close()

	sheetName := f.GetSheetName(0)
	rows, err := f.GetRows(sheetName)
	if err != nil {
		return fmt.Errorf("读取工作表失败: %v", err)
	}

	totalRows := len(rows) - 1 // 减去表头
	logInfo("总数据行数: %d", totalRows)

	if len(rows) < 1 {
		return fmt.Errorf("文件无数据")
	}

	// 加载断点续传状态
	state, err := loadState()
	if err != nil {
		return fmt.Errorf("加载处理状态失败: %v", err)
	}

	if state.LastProcessedRow > 0 {
		logInfo("从断点位置继续处理，上次处理到第 %d 行", state.LastProcessedRow)
	}

	// 检查是否是同一个文件的续传
	if state.FilePath != "" && state.FilePath != filePath {
		logInfo("检测到新文件，重置处理状态")
		state.LastProcessedRow = 0
	}
	state.FilePath = filePath

	// 创建列映射
	columnMap, err := createColumnMap(rows[0])
	if err != nil {
		return fmt.Errorf("创建列映射失败: %v", err)
	}
	logInfo("表头映射创建成功")

	// 从上次处理的位置继续
	dataRows := rows[1:][state.LastProcessedRow:]
	chunks := chunkData(dataRows, chunkSize)
	logInfo("数据已分块，共 %d 个块，每块 %d 条数据", len(chunks), chunkSize)

	rateLimiter := NewRateLimiter(50, 5*time.Second)
	// 控制协程数
	semaphore := make(chan struct{}, 6)
	var wg sync.WaitGroup

	// 添加错误处理通道
	errChan := make(chan error, 1)

	for i, chunk := range chunks {
		wg.Add(1)
		go func(chunk [][]string, chunkIndex int) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			startRow := state.LastProcessedRow + chunkIndex*chunkSize
			endRow := startRow + len(chunk)
			logInfo("开始处理第 %d 块数据 (行 %d-%d)", chunkIndex+1, startRow+1, endRow)

			if err := processChunk(chunk, columnMap, db, config, rateLimiter); err != nil {
				log.Printf("[ERROR] 分块处理失败: %v", err)
				select {
				case errChan <- err:
				default:
				}
				return
			}

			// 更新处理状态
			state.LastProcessedRow = state.LastProcessedRow + (chunkIndex+1)*chunkSize
			if err := saveState(*state); err != nil {
				log.Printf("[ERROR] 保存处理状态失败: %v", err)
			}
			logInfo("第 %d 块数据处理完成", chunkIndex+1)
		}(chunk, i)

		// 检查是否有错误发生
		select {
		case err := <-errChan:
			return fmt.Errorf("处理过程中出错: %v", err)
		default:
		}
	}

	wg.Wait()

	// 检查是否所有数据都处理完成
	if state.LastProcessedRow >= len(rows)-1 {
		os.Remove(".process_state")
		logInfo("文件 %s 已完全处理完成，共处理 %d 行数据", filePath, totalRows)
	} else {
		logInfo("文件 %s 部分处理完成，已处理到第 %d 行，总行数 %d",
			filePath, state.LastProcessedRow, totalRows)
	}

	// 最后检查是否有错误
	select {
	case err := <-errChan:
		return fmt.Errorf("处理过程中出错: %v", err)
	default:
		return nil
	}
}

func chunkData(data [][]string, size int) [][][]string {
	var chunks [][][]string
	for size < len(data) {
		data, chunks = data[size:], append(chunks, data[0:size])
	}
	chunks = append(chunks, data)
	return chunks
}

func processChunk(chunk [][]string, columnMap *ColumnMap, db *sql.DB, config Config, rateLimiter *RateLimiter) error {
	var rowsToInsert []map[string]interface{}
	skippedRows := 0

	for _, row := range chunk {
		data := processData(columnMap, row, config, rateLimiter)
		if data == nil {
			skippedRows++
			continue
		}
		rowsToInsert = append(rowsToInsert, data)
	}

	if len(rowsToInsert) > 0 {
		logInfo("准备插入 %d 条数据（跳过 %d 条无效数据）", len(rowsToInsert), skippedRows)
		if err := batchInsert(db, config.DBTable, config.DBFields, rowsToInsert); err != nil {
			return fmt.Errorf("批量插入失败: %v", err)
		}
		logInfo("成功插入 %d 条数据", len(rowsToInsert))
	} else {
		logInfo("本批次无有效数据需要插入（跳过 %d 条无效数据）", skippedRows)
	}

	return nil
}

// 数据处理逻辑
func processData(columnMap *ColumnMap, row []string, config Config, rateLimiter *RateLimiter) map[string]interface{} {
	if checkData(columnMap, row) {
		return nil
	}

	imageData, err := checkImageURL(row[columnMap.Default_pic])
	if err != nil {
		log.Printf("图片检查失败: %v", err)
		return nil
	}

	fileName, err := extractFileName(row[columnMap.Default_pic])
	if err != nil {
		log.Printf("提取文件名失败: %v", err)
		return nil
	}

	// 在上传前获取令牌
	rateLimiter.acquire()

	ossURL, err := uploadToOSS(imageData,
		fmt.Sprintf("%s.jpg", generateHash(fileName)),
		config.OSS,
		config.OssUploadDir)
	if err != nil {
		log.Printf("上传到 OSS 失败: %v", err)
		return nil
	}

	// 构建地址
	address := strings.TrimSpace(strings.Join([]string{
		getValue(row, columnMap.Province),
		getValue(row, columnMap.City),
		getValue(row, columnMap.Regionname),
	}, ""))

	return map[string]interface{}{
		"type":                  2,
		"cate_id":               config.CateID,
		"name":                  formatName(row[columnMap.Name], getValue(row, columnMap.Branchname)),
		"address":               address,
		"detailed_address":      getValue(row, columnMap.Address),
		"image":                 ossURL,
		"latitude":              row[columnMap.Latitude],
		"longitude":             row[columnMap.Longitude],
		"day_time":              "08:00:00 - 24:00:00",
		"day_start":             "08:00:00",
		"day_end":               "24:00:00",
		"add_time":              time.Now().Unix(),
		"is_show":               1,
		"is_del":                0,
		"is_store":              1,
		"default_delivery":      2,
		"product_verify_status": 1,
		"agent_type":            4,
		"is_virtual":            1,
	}
}

// 添加辅助函数来安全地获取值
func getValue(row []string, index int) string {
	if index >= 0 && index < len(row) {
		return row[index]
	}
	return ""
}

// 修改 checkData 函数
func checkData(columnMap *ColumnMap, row []string) bool {
	if columnMap.Province >= 0 && shouldSkipRow(row[columnMap.Province]) {
		return true
	}

	// 检查经纬度
	if getValue(row, columnMap.Longitude) == "" || getValue(row, columnMap.Latitude) == "" {
		return true
	}

	// 检查图片地址
	if getValue(row, columnMap.Default_pic) == "" {
		return true
	}

	return false
}

// 判断是否需要跳过行
func shouldSkipRow(region string) bool {
	skipRegions := map[string]bool{
		"香港": true,
		"台湾": true,
	}

	return skipRegions[region]
}

func batchInsert(db *sql.DB, tableName string, dbFields []string, data []map[string]interface{}) error {
	if len(data) == 0 {
		return nil
	}

	columns := strings.Join(dbFields, ", ")
	placeholders := "(" + strings.Repeat("?, ", len(dbFields)-1) + "?)"
	valuePlaceholders := strings.Repeat(placeholders+",", len(data))
	valuePlaceholders = strings.TrimRight(valuePlaceholders, ",")

	sqlStr := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tableName, columns, valuePlaceholders)

	var values []interface{}
	for _, row := range data {
		for _, field := range dbFields {
			values = append(values, row[field])
		}
	}

	_, err := db.Exec(sqlStr, values...)
	return err
}

// 检查图片地址是否可用，并获取内容
func checkImageURL(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("请求失败: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("图片不可用，状态码: %d", resp.StatusCode)
	}

	// 读取图片内容
	imageData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取图片内容失败: %v", err)
	}

	return imageData, nil
}

// 上传图片到阿里云 OSS
func uploadToOSS(imageData []byte, fileName string, config OSSConfig, ossUploadDir string) (string, error) {
	const maxRetries = 3
	var lastErr error

	logInfo("开始上传文件到OSS: %s", fileName)

	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			logInfo("第 %d 次重试上传...", i)
		}

		client, err := oss.New(config.Endpoint, config.AccessKeyID, config.AccessKeySecret)
		if err != nil {
			lastErr = err
			time.Sleep(time.Second * time.Duration(i+1))
			continue
		}

		bucket, err := client.Bucket(config.BucketName)
		if err != nil {
			lastErr = err
			time.Sleep(time.Second * time.Duration(i+1))
			continue
		}

		objectKey := fmt.Sprintf("upload/"+ossUploadDir+"/%s", fileName)
		if err := bucket.PutObject(objectKey, bytes.NewReader(imageData)); err != nil {
			lastErr = err
			log.Printf("[ERROR] 上传失败: %v, 准备重试...", err)
			time.Sleep(time.Second * time.Duration(i+1))
			continue
		}

		url := fmt.Sprintf("https://%s.%s/%s",
			config.BucketName,
			strings.TrimPrefix(config.Endpoint, "https://"),
			objectKey)
		logInfo("文件上传成功: %s", url)
		return url, nil
	}

	return "", fmt.Errorf("上传失败，重试次数已用完: %v", lastErr)
}

// 修改 generateHash 函数，使用精确到秒的时间戳
func generateHash(fileName string) string {
	// 使用精确到秒的时间戳
	timestamp := time.Now().Format("20060102150405") // 年月日时分秒
	// 添加一个随机数来进一步确保唯一性
	randomStr := fmt.Sprintf("%d", time.Now().UnixNano()%1000)
	data := fmt.Sprintf("%s-%s-%s", fileName, timestamp, randomStr)
	hash := md5.Sum([]byte(data))
	return hex.EncodeToString(hash[:])
}

// 从 URL 中提取文件名
func extractFileName(imageURL string) (string, error) {
	// 解析 URL
	parsedURL, err := url.Parse(imageURL)
	if err != nil {
		return "", fmt.Errorf("解析 URL 失败: %v", err)
	}

	// 获取路径部分的最后一部分作为文件名
	fileName := path.Base(parsedURL.Path)
	if fileName == "" {
		return "", fmt.Errorf("无法从 URL 提取文件名")
	}

	return fileName, nil
}

// 添加辅助函数
func formatName(name, branchname string) string {
	if branchname != "" {
		return strings.TrimSpace(name) + "-" + strings.TrimSpace(branchname)
	}
	return name
}
