package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/xiaocaoooo/amiabot-plugin-sdk/onebot/ob11"
	papi "github.com/xiaocaoooo/amiabot-plugin-sdk/plugin"
	"github.com/xiaocaoooo/amiabot-plugin-sdk/plugin/transport"
	"github.com/xiaocaoooo/amiabot-plugin-sdk/util"

	_ "github.com/lib/pq"
)

const (
	requestPattern = `^gn\s+request\s*(.+)$`
	approvePattern = `^gn\s+approve$`
	listPattern    = `^gn\s+list$`
	helpPattern    = `^gn(?:\s+help)?$`
)

// GNPlugin 群名称定时修改插件
type GNPlugin struct {
	mu  sync.RWMutex
	cfg Config
	db  *sql.DB
}

// Config 插件配置
type Config struct {
	AdminQQ     int64  `json:"admin_qq"`
	ReplaceText string `json:"replace_text"`
	GroupID     int64  `json:"group_id"`
	PGSQLURI    string `json:"pgsql_uri"`
}

// GroupNameRequest 群名称请求记录
type GroupNameRequest struct {
	ID             int64      `json:"id"`
	GroupID        int64      `json:"group_id"`
	RequesterID    int64      `json:"requester_id"`
	NameTemplate   string     `json:"name_template"`
	FinalName      string     `json:"final_name"`
	Status         string     `json:"status"`
	CreatedAt      time.Time  `json:"created_at"`
	ApprovedBy     *int64     `json:"approved_by"`
	ApprovedAt     *time.Time `json:"approved_at"`
	ExecutedAt     *time.Time `json:"executed_at"`
	MessageGroupID *int64     `json:"message_group_id"`
	MessageSeq     *int64     `json:"message_seq"`
}

type eventContext struct {
	MsgType string
	GroupID any
	UserID  any
	Content string
	Payload map[string]any
	Message []map[string]any
}

func (p *GNPlugin) Descriptor(ctx context.Context) (papi.Descriptor, error) {
	_ = ctx
	schema := json.RawMessage(`{
		"type": "object",
		"properties": {
			"admin_qq": {"type": "integer", "description": "管理员QQ号，只有该用户可执行approve命令"},
			"replace_text": {"type": "string", "description": "用于替换%s的文本"},
			"group_id": {"type": "integer", "description": "目标群ID（用于定时任务执行群名修改）"},
			"pgsql_uri": {"type": "string", "description": "PostgreSQL数据库连接URI"}
		},
		"required": ["admin_qq", "replace_text", "group_id", "pgsql_uri"],
		"additionalProperties": false
	}`)
	def := json.RawMessage(`{"admin_qq": 0, "replace_text": "", "group_id": 0, "pgsql_uri": ""}`)

	return papi.Descriptor{
		Name:         "XBot GN",
		PluginID:     "external.xbot-gn",
		Version:      "1.1.0",
		Author:       "nyanyabot",
		Description:  "群名称定时修改插件 - 每天UTC+8 00:00执行一个已审核的群名",
		Dependencies: []string{},
		Exports:      []papi.ExportSpec{},
		Config: &papi.ConfigSpec{
			Version:     "1",
			Description: "XBot GN plugin config",
			Schema:      schema,
			Default:     def,
		},
		Commands: []papi.CommandListener{
			{
				Name:        "gn-request",
				ID:          "cmd.gn-request",
				Description: "提交群名称修改请求 (如: gn request 今日%s日)",
				Pattern:     requestPattern,
				MatchRaw:    false,
				Handler:     "HandleRequest",
			},
			{
				Name:        "gn-approve",
				ID:          "cmd.gn-approve",
				Description: "管理员批准引用的群名称修改请求 (回复request消息后: gn approve)",
				Pattern:     approvePattern,
				MatchRaw:    false,
				Handler:     "HandleApprove",
			},
			{
				Name:        "gn-list",
				ID:          "cmd.gn-list",
				Description: "列出所有已通过审核的群名称 (如: gn list)",
				Pattern:     listPattern,
				MatchRaw:    false,
				Handler:     "HandleList",
			},
			{
				Name:        "gn-help",
				ID:          "cmd.gn-help",
				Description: "显示帮助信息 (如: gn 或 gn help)",
				Pattern:     helpPattern,
				MatchRaw:    false,
				Handler:     "HandleHelp",
			},
		},
		Crons: []papi.CronListener{
			{
				Name:        "execute_group_name",
				ID:          "cron.execute_group_name",
				Description: "每天UTC+8 00:00执行一个已审核的群名修改",
				Schedule:    "0 0 0 * * *", // 秒 分 时 日 月 周 (UTC+8 00:00:00)
				Handler:     "HandleCronExecute",
			},
		},
	}, nil
}

func (p *GNPlugin) Configure(ctx context.Context, config json.RawMessage) error {
	_ = ctx
	cfg := Config{}
	if len(config) > 0 {
		_ = json.Unmarshal(config, &cfg)
	}

	p.mu.Lock()
	p.cfg = cfg
	p.mu.Unlock()

	// Initialize database connection
	if cfg.PGSQLURI != "" {
		db, err := sql.Open("postgres", cfg.PGSQLURI)
		if err != nil {
			return fmt.Errorf("failed to connect to database: %w", err)
		}
		if err := db.Ping(); err != nil {
			return fmt.Errorf("failed to ping database: %w", err)
		}
		p.db = db

		// Create tables if not exist
		if err := p.initTables(); err != nil {
			return fmt.Errorf("failed to init tables: %w", err)
		}
	}

	return nil
}

func (p *GNPlugin) initTables() error {
	_, err := p.db.Exec(`
		CREATE TABLE IF NOT EXISTS group_name_requests (
			id SERIAL PRIMARY KEY,
			group_id BIGINT NOT NULL,
			requester_id BIGINT NOT NULL,
			name_template TEXT NOT NULL,
			final_name TEXT NOT NULL,
			status VARCHAR(20) DEFAULT 'pending',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			approved_by BIGINT,
			approved_at TIMESTAMP,
			executed_at TIMESTAMP,
			message_group_id BIGINT,
			message_seq BIGINT
		)
	`)
	if err != nil {
		return err
	}

	// Add columns if table exists but columns don't (migration)
	_, _ = p.db.Exec(`ALTER TABLE group_name_requests ADD COLUMN IF NOT EXISTS message_group_id BIGINT`)
	_, _ = p.db.Exec(`ALTER TABLE group_name_requests ADD COLUMN IF NOT EXISTS message_seq BIGINT`)

	_, err = p.db.Exec(`CREATE INDEX IF NOT EXISTS idx_requests_status ON group_name_requests(status)`)
	if err != nil {
		return err
	}

	_, err = p.db.Exec(`CREATE INDEX IF NOT EXISTS idx_requests_created ON group_name_requests(created_at)`)
	if err != nil {
		return err
	}

	_, err = p.db.Exec(`CREATE INDEX IF NOT EXISTS idx_requests_group_seq ON group_name_requests(message_group_id, message_seq) WHERE message_group_id IS NOT NULL AND message_seq IS NOT NULL`)
	if err != nil {
		return err
	}

	_, err = p.db.Exec(`CREATE INDEX IF NOT EXISTS idx_requests_status_approved ON group_name_requests(status, approved_at) WHERE status = 'approved' AND executed_at IS NULL`)
	return err
}

func (p *GNPlugin) Invoke(ctx context.Context, method string, paramsJSON json.RawMessage, callerPluginID string) (json.RawMessage, error) {
	_ = ctx
	_ = method
	_ = paramsJSON
	_ = callerPluginID
	return nil, papi.NewStructuredError(papi.ErrorCodeNotFound, "method is not exported")
}

func (p *GNPlugin) Handle(ctx context.Context, listenerID string, eventRaw ob11.Event, match *papi.CommandMatch) (papi.HandleResult, error) {
	switch listenerID {
	case "cmd.gn-request":
		return p.handleRequest(ctx, eventRaw, match)
	case "cmd.gn-approve":
		return p.handleApprove(ctx, eventRaw)
	case "cmd.gn-list":
		return p.handleList(ctx, eventRaw)
	case "cmd.gn-help":
		return p.handleHelp(ctx, eventRaw)
	case "cron.execute_group_name":
		return p.handleCronExecute(ctx, eventRaw)
	default:
		return papi.HandleResult{}, nil
	}
}

func (p *GNPlugin) Shutdown(ctx context.Context) error {
	_ = ctx
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

// handleRequest 处理提交群名称修改请求
func (p *GNPlugin) handleRequest(ctx context.Context, eventRaw ob11.Event, match *papi.CommandMatch) (papi.HandleResult, error) {
	_ = ctx
	log := hclog.L()
	evt, err := parseEventContext(eventRaw)
	if err != nil {
		log.Error("[XBot GN] 解析事件失败", "error", err)
		return papi.HandleResult{}, nil
	}

	host := transport.Host()
	if host == nil {
		log.Warn("[XBot GN] host 为 nil，终止")
		return papi.HandleResult{}, nil
	}

	// 白名单检查：只有配置中的 group_id 才能使用此插件
	groupID := anyToInt64(evt.GroupID)
	p.mu.RLock()
	allowedGroupID := p.cfg.GroupID
	replaceText := p.cfg.ReplaceText
	p.mu.RUnlock()

	if evt.MsgType != "group" || groupID != allowedGroupID {
		// 不在白名单群聊中，静默忽略
		return papi.HandleResult{}, nil
	}

	// Extract the name template from match
	if match == nil || len(match.Groups) < 1 {
		util.SendText(host, evt.MsgType, evt.GroupID, evt.UserID, "❌ 请提供群名称模板")
		return papi.HandleResult{}, nil
	}

	nameTemplate := strings.TrimSpace(match.Groups[0])
	if nameTemplate == "" {
		util.SendText(host, evt.MsgType, evt.GroupID, evt.UserID, "❌ 群名称模板不能为空")
		return papi.HandleResult{}, nil
	}

	// Check if template contains %s
	if !strings.Contains(nameTemplate, "%s") {
		util.SendText(host, evt.MsgType, evt.GroupID, evt.UserID, "❌ 群名称模板必须包含 %s")
		return papi.HandleResult{}, nil
	}

	// Replace %s with configured text
	finalName := strings.ReplaceAll(nameTemplate, "%s", replaceText)

	// Insert into database
	requesterID := anyToInt64(evt.UserID)

	// Get message identifiers from event (for reply tracking)
	msgGroupID, msgSeq := extractMessageIdentifier(evt)

	var requestID int64
	err = p.db.QueryRow(`
		INSERT INTO group_name_requests (
			group_id, requester_id, name_template, final_name, 
			message_group_id, message_seq
		)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id
	`, groupID, requesterID, nameTemplate, finalName, msgGroupID, msgSeq).Scan(&requestID)
	if err != nil {
		log.Error("[XBot GN] 插入请求失败", "error", err)
		util.SendError(host, evt.MsgType, evt.GroupID, evt.UserID, "❌ 提交请求失败", err)
		return papi.HandleResult{}, nil
	}

	util.SendText(host, evt.MsgType, evt.GroupID, evt.UserID,
		fmt.Sprintf("✅ 请求 #%d 已提交！\n模板: %s\n最终群名: %s\n请等待管理员审核（管理员请回复此消息后发送: gn approve）",
			requestID, nameTemplate, finalName))
	return papi.HandleResult{}, nil
}

// handleApprove 处理管理员批准请求
func (p *GNPlugin) handleApprove(ctx context.Context, eventRaw ob11.Event) (papi.HandleResult, error) {
	log := hclog.L()
	evt, err := parseEventContext(eventRaw)
	if err != nil {
		log.Error("[XBot GN] 解析事件失败", "error", err)
		return papi.HandleResult{}, nil
	}

	host := transport.Host()
	if host == nil {
		log.Warn("[XBot GN] host 为 nil，终止")
		return papi.HandleResult{}, nil
	}

	// 白名单检查：只有配置中的 group_id 才能使用此插件
	groupID := anyToInt64(evt.GroupID)
	p.mu.RLock()
	allowedGroupID := p.cfg.GroupID
	adminQQ := p.cfg.AdminQQ
	p.mu.RUnlock()

	if evt.MsgType != "group" || groupID != allowedGroupID {
		// 不在白名单群聊中，静默忽略
		return papi.HandleResult{}, nil
	}

	// Check if sender is admin
	senderID := anyToInt64(evt.UserID)
	if senderID != adminQQ {
		util.SendText(host, evt.MsgType, evt.GroupID, evt.UserID, "❌ 只有管理员可以执行此命令")
		return papi.HandleResult{}, nil
	}

	// Extract reply message identifier from the event
	replyGroupID, replySeq, found := extractReplyMessageIdentifier(ctx, host, evt)
	if !found {
		util.SendText(host, evt.MsgType, evt.GroupID, evt.UserID, "❌ 请回复要审核的请求消息后再发送 gn approve")
		return papi.HandleResult{}, nil
	}

	// Find the request by the replied message identifier
	var requestID int64
	var nameTemplate, finalName string
	err = p.db.QueryRow(`
		SELECT id, name_template, final_name FROM group_name_requests
		WHERE message_group_id = $1 AND message_seq = $2
		AND status = 'pending'
		LIMIT 1
	`, replyGroupID, replySeq).Scan(&requestID, &nameTemplate, &finalName)
	if err != nil {
		if err == sql.ErrNoRows {
			util.SendText(host, evt.MsgType, evt.GroupID, evt.UserID, "❌ 未找到对应的待审核请求，请确认回复的是 gn request 消息")
			util.SendText(host, evt.MsgType, evt.GroupID, evt.UserID, fmt.Sprintf("Debug: reply seq=%d", replySeq))
		} else {
			log.Error("[XBot GN] 查询请求失败", "error", err)
			util.SendError(host, evt.MsgType, evt.GroupID, evt.UserID, "❌ 查询请求失败", err)
		}
		return papi.HandleResult{}, nil
	}

	// Approve the request
	_, err = p.db.Exec(`
		UPDATE group_name_requests 
		SET status = 'approved', approved_by = $1, approved_at = CURRENT_TIMESTAMP 
		WHERE id = $2
	`, senderID, requestID)
	if err != nil {
		log.Error("[XBot GN] 更新请求状态失败", "error", err)
		util.SendError(host, evt.MsgType, evt.GroupID, evt.UserID, "❌ 审核失败", err)
		return papi.HandleResult{}, nil
	}

	util.SendText(host, evt.MsgType, evt.GroupID, evt.UserID,
		fmt.Sprintf("✅ 已批准请求 #%d！\n模板: %s\n最终群名: %s\n将在UTC+8 00:00自动修改群名称。", requestID, nameTemplate, finalName))
	return papi.HandleResult{}, nil
}

// handleList 列出所有已通过审核待执行的群名称
func (p *GNPlugin) handleList(ctx context.Context, eventRaw ob11.Event) (papi.HandleResult, error) {
	_ = ctx
	log := hclog.L()
	evt, err := parseEventContext(eventRaw)
	if err != nil {
		log.Error("[XBot GN] 解析事件失败", "error", err)
		return papi.HandleResult{}, nil
	}

	host := transport.Host()
	if host == nil {
		log.Warn("[XBot GN] host 为 nil，终止")
		return papi.HandleResult{}, nil
	}

	// 白名单检查：只有配置中的 group_id 才能使用此插件
	groupID := anyToInt64(evt.GroupID)
	p.mu.RLock()
	allowedGroupID := p.cfg.GroupID
	p.mu.RUnlock()

	if evt.MsgType != "group" || groupID != allowedGroupID {
		// 不在白名单群聊中，静默忽略
		return papi.HandleResult{}, nil
	}

	// Query approved (not yet executed) requests
	rows, err := p.db.Query(`
		SELECT id, requester_id, name_template, final_name, created_at, approved_by, approved_at
		FROM group_name_requests
		WHERE status = 'approved' AND executed_at IS NULL
		ORDER BY approved_at ASC
	`)
	if err != nil {
		log.Error("[XBot GN] 查询请求列表失败", "error", err)
		util.SendError(host, evt.MsgType, evt.GroupID, evt.UserID, "❌ 查询列表失败", err)
		return papi.HandleResult{}, nil
	}
	defer rows.Close()

	var requests []GroupNameRequest
	for rows.Next() {
		var r GroupNameRequest
		if err := rows.Scan(&r.ID, &r.RequesterID, &r.NameTemplate, &r.FinalName, &r.CreatedAt, &r.ApprovedBy, &r.ApprovedAt); err != nil {
			log.Error("[XBot GN] 扫描请求失败", "error", err)
			continue
		}
		requests = append(requests, r)
	}

	if len(requests) == 0 {
		util.SendText(host, evt.MsgType, evt.GroupID, evt.UserID, "ℹ️ 当前没有已通过审核待执行的群名称")
		return papi.HandleResult{}, nil
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("📋 已通过审核待执行的群名称 (%d条):\n\n", len(requests)))
	for i, r := range requests {
		approvedBy := "未知"
		if r.ApprovedBy != nil {
			approvedBy = strconv.FormatInt(*r.ApprovedBy, 10)
		}
		approvedAt := "未知"
		if r.ApprovedAt != nil {
			approvedAt = r.ApprovedAt.Format("01-02 15:04")
		}
		sb.WriteString(fmt.Sprintf("%d. #%d | 申请人: %d\n   模板: %s\n   结果: %s\n   审核: %s @ %s\n\n",
			i+1, r.ID, r.RequesterID, r.NameTemplate, r.FinalName, approvedBy, approvedAt))
	}

	util.SendText(host, evt.MsgType, evt.GroupID, evt.UserID, sb.String())
	return papi.HandleResult{}, nil
}

// handleHelp 显示帮助信息
func (p *GNPlugin) handleHelp(ctx context.Context, eventRaw ob11.Event) (papi.HandleResult, error) {
	_ = ctx
	log := hclog.L()
	evt, err := parseEventContext(eventRaw)
	if err != nil {
		log.Error("[XBot GN] 解析事件失败", "error", err)
		return papi.HandleResult{}, nil
	}

	host := transport.Host()
	if host == nil {
		log.Warn("[XBot GN] host 为 nil，终止")
		return papi.HandleResult{}, nil
	}

	// 白名单检查：只有配置中的 group_id 才能使用此插件
	groupID := anyToInt64(evt.GroupID)
	p.mu.RLock()
	allowedGroupID := p.cfg.GroupID
	p.mu.RUnlock()

	if evt.MsgType != "group" || groupID != allowedGroupID {
		// 不在白名单群聊中，静默忽略
		return papi.HandleResult{}, nil
	}

	helpText := `群名称修改插件

可用命令:
• gn request <模板> - 提交群名称修改请求
  模板中必须包含 %s，会被替换为配置的文本
  例: gn request 今日%s日

• gn approve - 管理员批准引用的请求
  操作: 回复 gn request 的机器人消息，然后发送 gn approve

• gn list - 列出所有已通过审核待执行的群名称

• gn help - 显示本帮助信息

注意:
- 请求需管理员审核后，会在UTC+8 00:00自动执行
- 每天只执行一个群名，其余留到第二天
- 本插件只在配置的群聊中生效`

	util.SendText(host, evt.MsgType, evt.GroupID, evt.UserID, helpText)
	return papi.HandleResult{}, nil
}

// handleCronExecute 定时任务：每天执行一个已审核的群名
func (p *GNPlugin) handleCronExecute(ctx context.Context, eventRaw ob11.Event) (papi.HandleResult, error) {
	_ = ctx
	log := hclog.L()

	host := transport.Host()
	if host == nil {
		log.Warn("[XBot GN] host 为 nil，无法执行定时任务")
		return papi.HandleResult{}, nil
	}

	p.mu.RLock()
	groupID := p.cfg.GroupID
	p.mu.RUnlock()

	// Query only the earliest approved request (LIMIT 1)
	var id int64
	var finalName string
	err := p.db.QueryRow(`
		SELECT id, final_name FROM group_name_requests
		WHERE status = 'approved' AND executed_at IS NULL
		ORDER BY approved_at ASC
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`).Scan(&id, &finalName)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Info("[XBot GN] No pending approved requests to execute")
		} else {
			log.Error("[XBot GN] Failed to query approved requests", "error", err)
		}
		return papi.HandleResult{}, nil
	}

	// Call OneBot API to set group name
	_, err = host.CallOneBot(context.Background(), "set_group_name", map[string]any{
		"group_id":   groupID,
		"group_name": finalName,
	})
	if err != nil {
		log.Error("[XBot GN] Failed to set group name", "id", id, "name", finalName, "error", err)

		// 标记为失败
		_, _ = p.db.Exec(`
			UPDATE group_name_requests 
			SET status = 'failed', executed_at = CURRENT_TIMESTAMP 
			WHERE id = $1
		`, id)

		// 通知管理员
		p.mu.RLock()
		adminQQ := p.cfg.AdminQQ
		p.mu.RUnlock()

		if adminQQ > 0 {
			util.SendText(host, "private", 0, &adminQQ,
				fmt.Sprintf("❌ 定时任务执行失败\n请求ID: %d\n群名: %s\n错误: %v", id, finalName, err))
		}

		return papi.HandleResult{}, nil
	}

	// Mark as executed
	_, err = p.db.Exec(`
		UPDATE group_name_requests 
		SET status = 'executed', executed_at = CURRENT_TIMESTAMP 
		WHERE id = $1
	`, id)
	if err != nil {
		log.Error("[XBot GN] Failed to mark request as executed", "id", id, "error", err)
		return papi.HandleResult{}, nil
	}

	log.Info("[XBot GN] Executed request", "id", id, "final_name", finalName)
	return papi.HandleResult{}, nil
}

func parseEventContext(eventRaw ob11.Event) (eventContext, error) {
	var evt map[string]any
	dec := json.NewDecoder(bytes.NewReader(eventRaw))
	dec.UseNumber()
	if err := dec.Decode(&evt); err != nil {
		return eventContext{}, err
	}

	eCtx := eventContext{
		MsgType: strings.TrimSpace(anyToString(evt["message_type"])),
		GroupID: evt["group_id"],
		UserID:  evt["user_id"],
		Content: strings.TrimSpace(anyToString(evt["content"])),
		Payload: evt,
	}

	// Extract message segments for reply detection
	if msg, ok := evt["message"].([]any); ok {
		for _, seg := range msg {
			if segMap, ok := seg.(map[string]any); ok {
				eCtx.Message = append(eCtx.Message, segMap)
			}
		}
	}

	return eCtx, nil
}

// extractMessageIdentifier 从事件中提取消息标识（group_id + message_seq）
func extractMessageIdentifier(evt eventContext) (groupID int64, messageSeq int64) {
	groupID = anyToInt64(evt.GroupID)
	messageSeq = anyToInt64(evt.Payload["real_seq"])
	return
}

// extractReplyMessageIdentifier 从回复消息段中提取消息标识
// 通过调用 get_msg API 获取完整消息信息
// 返回: (groupID, messageSeq, found)
func extractReplyMessageIdentifier(ctx context.Context, host util.HostCaller, evt eventContext) (int64, int64, bool) {
	log := hclog.L()
	// 1. 从 reply 段提取 message_id
	var replyMsgID int64
	for _, seg := range evt.Message {
		msgType, ok := seg["type"].(string)
		if !ok || msgType != "reply" {
			continue
		}

		data, ok := seg["data"].(map[string]any)
		if !ok {
			continue
		}

		// 提取 id 或 message_id
		if idStr, ok := data["id"].(string); ok && idStr != "" {
			replyMsgID, _ = strconv.ParseInt(idStr, 10, 64)
			break
		}
		if idNum, ok := data["id"].(json.Number); ok {
			replyMsgID, _ = idNum.Int64()
			break
		}
		if idFloat, ok := data["id"].(float64); ok {
			replyMsgID = int64(idFloat)
			break
		}
	}

	if replyMsgID == 0 {
		// 也检查 payload 中的 reply 字段
		if reply, ok := evt.Payload["reply"]; ok {
			replyMsgID = anyToInt64(reply)
		}
	}

	if replyMsgID == 0 {
		return 0, 0, false
	}

	// 2. 调用 get_msg API 获取完整消息
	resp, err := host.CallOneBot(ctx, "get_msg", map[string]any{
		"message_id": replyMsgID,
	})
	if err != nil {
		log.Warn("[extractReplyMessageIdentifier] get_msg API 调用失败", "error", err)
		return 0, 0, false
	}

	// 检查 resp.Data 是否为空
	if len(resp.Data) == 0 {
		log.Warn("[extractReplyMessageIdentifier] get_msg API 返回空数据")
		return 0, 0, false
	}

	// 3. 解析返回结果
	var result map[string]any
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		log.Warn("[extractReplyMessageIdentifier] 解析 get_msg 响应失败", "error", err)
		return 0, 0, false
	}

	// 4. 提取 group_id 和 message_seq
	groupID := anyToInt64(result["group_id"])
	messageSeq := anyToInt64(result["real_seq"])

	if groupID > 0 && messageSeq > 0 {
		return groupID, messageSeq, true
	}

	log.Warn("[extractReplyMessageIdentifier] 无法提取 group_id 或 message_seq")
	return 0, 0, false
}

func anyToString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return x.String()
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32)
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case uint32:
		return strconv.FormatUint(uint64(x), 10)
	case bool:
		return strconv.FormatBool(x)
	default:
		return ""
	}
}

func anyToInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case float64:
		return int64(x)
	case json.Number:
		n, _ := x.Int64()
		return n
	case string:
		n, _ := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		return n
	default:
		return 0
	}
}

func main() {
	logger := hclog.New(&hclog.LoggerOptions{
		Name:  "nyanyabot-plugin-xbot-gn",
		Level: hclog.Info,
	})

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: transport.Handshake(),
		Plugins: plugin.PluginSet{
			transport.PluginName: &transport.Map{PluginImpl: &GNPlugin{}},
		},
		Logger: logger,
	})
}
