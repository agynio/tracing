//go:build e2e

package e2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	tracingv1 "github.com/agynio/tracing/.gen/go/agynio/api/tracing/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

const (
	spanInvocationMessage = "invocation.message"
	spanLLMCall           = "llm.call"
	spanToolExecution     = "tool.execution"
	spanSummarization     = "summarization"

	attrThreadID                   = "agyn.thread.id"
	attrOrganizationID             = "agyn.organization.id"
	attrMessageID                  = "agyn.thread.message.id"
	attrMessageText                = "agyn.message.text"
	attrMessageRole                = "agyn.message.role"
	attrMessageKind                = "agyn.message.kind"
	attrLLMSystem                  = "gen_ai.system"
	attrLLMRequestModel            = "gen_ai.request.model"
	attrLLMFinishReason            = "gen_ai.response.finish_reason"
	attrLLMResponseText            = "agyn.llm.response_text"
	attrUsageInputTokens           = "gen_ai.usage.input_tokens"
	attrUsageOutputTokens          = "gen_ai.usage.output_tokens"
	attrUsageCacheReadInputTokens  = "gen_ai.usage.cache_read.input_tokens"
	attrUsageReasoningTokens       = "agyn.usage.reasoning_tokens"
	attrToolName                   = "agyn.tool.name"
	attrToolInput                  = "agyn.tool.input"
	attrToolOutput                 = "agyn.tool.output"
	attrToolCallID                 = "agyn.tool.call_id"
	attrSummarizationText          = "agyn.summarization.text"
	attrSummarizationNewContext    = "agyn.summarization.new_context_count"
	attrSummarizationOldContext    = "agyn.summarization.old_context_tokens"
	eventLLMContextItem            = "agyn.llm.context_item"
	attrContextRole                = "agyn.context.role"
	attrContextText                = "agyn.context.text"
	attrContextIsNew               = "agyn.context.is_new"
	attrContextSizeBytes           = "agyn.context.size_bytes"
	defaultLLMSystem               = "openai"
	defaultLLMModel                = "gpt-4"
	defaultLLMFinishReason         = "stop"
	defaultLLMResponseText         = "Hello from the model"
	defaultToolName                = "shell_command"
	defaultSummarizationText       = "Summary of conversation"
	defaultInvocationMessageText   = "Hello there"
	defaultInvocationMessageRole   = "user"
	defaultInvocationMessageKind   = "source"
	defaultLLMToolCallResponseText = "Tool results"
	defaultOrganizationID          = "11111111-1111-1111-1111-111111111111"
	otherOrganizationID            = "22222222-2222-2222-2222-222222222222"
	defaultMessageID               = "33333333-3333-3333-3333-333333333333"
	otherMessageID                 = "44444444-4444-4444-4444-444444444444"
	missingMessageID               = "55555555-5555-5555-5555-555555555555"
)

func TestIngestAndQuerySimpleTrace(t *testing.T) {
	traceID := newTraceID()
	spanID := newSpanID()
	threadID := fmt.Sprintf("thread-%x", traceID)
	startNs := uint64(time.Now().UnixNano())
	span := buildSpan(
		spanInvocationMessage,
		traceID,
		spanID,
		startNs,
		startNs+1_000_000,
		invocationMessageAttrs(defaultInvocationMessageText),
	)
	resourceSpans := buildResourceSpans([]*tracev1.Span{span}, resourceAttrs(threadID))

	collectorClient, queryClient := newTracingClients(t)
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	resp, err := exportSpans(ctx, collectorClient, []*tracev1.ResourceSpans{resourceSpans})
	require.NoError(t, err)
	require.Equal(t, int64(0), resp.GetPartialSuccess().GetRejectedSpans())

	requireEventually(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		summary, err := queryClient.GetTraceSummary(ctx, &tracingv1.GetTraceSummaryRequest{TraceId: traceID})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		assert.Equal(c, int64(1), summary.GetTotalSpans())
		assert.Equal(c, int64(1), summary.GetCountsByName()[spanInvocationMessage])
	})

	requireEventually(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		listResp, err := queryClient.ListSpans(ctx, &tracingv1.ListSpansRequest{
			OrganizationId: defaultOrganizationID,
			Filter:         &tracingv1.SpanFilter{TraceId: traceID},
		})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		spans := flattenSpans(listResp.GetResourceSpans())
		if !assert.Len(c, spans, 1) {
			return
		}
		assert.Equal(c, spanInvocationMessage, spans[0].GetName())
		assert.Equal(c, spanID, spans[0].GetSpanId())
	})

	requireEventually(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		spanResp, err := queryClient.GetSpan(ctx, &tracingv1.GetSpanRequest{TraceId: traceID, SpanId: spanID})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		span := singleSpan(c, spanResp.GetResourceSpans())
		if span == nil {
			return
		}
		assertStringAttr(c, span.GetAttributes(), attrMessageText, defaultInvocationMessageText)
		assertStringAttr(c, span.GetAttributes(), attrMessageRole, defaultInvocationMessageRole)
		assertStringAttr(c, span.GetAttributes(), attrMessageKind, defaultInvocationMessageKind)
		resource := firstResource(c, spanResp.GetResourceSpans())
		if resource == nil {
			return
		}
		assertStringAttr(c, resource.GetAttributes(), attrThreadID, threadID)
	})
}

func TestIngestLLMCallSpan(t *testing.T) {
	traceID := newTraceID()
	spanID := newSpanID()
	threadID := fmt.Sprintf("thread-%x", traceID)
	startNs := uint64(time.Now().UnixNano())
	span := buildSpan(
		spanLLMCall,
		traceID,
		spanID,
		startNs,
		startNs+2_000_000,
		llmCallAttrs(defaultLLMResponseText, 100, 50, 10, 5),
	)
	resourceSpans := buildResourceSpans([]*tracev1.Span{span}, resourceAttrs(threadID))

	collectorClient, queryClient := newTracingClients(t)
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	resp, err := exportSpans(ctx, collectorClient, []*tracev1.ResourceSpans{resourceSpans})
	require.NoError(t, err)
	require.Equal(t, int64(0), resp.GetPartialSuccess().GetRejectedSpans())

	requireEventually(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		totalsResp, err := queryClient.GetTraceSpanTotals(ctx, &tracingv1.GetTraceSpanTotalsRequest{TraceId: traceID})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		usage := totalsResp.GetTokenUsage()
		if !assert.NotNil(c, usage) {
			return
		}
		assert.Equal(c, int64(1), totalsResp.GetSpanCount())
		assert.Equal(c, int64(100), usage.GetInputTokens())
		assert.Equal(c, int64(50), usage.GetOutputTokens())
		assert.Equal(c, int64(10), usage.GetCacheReadInputTokens())
		assert.Equal(c, int64(5), usage.GetReasoningTokens())
		assert.Equal(c, int64(150), usage.GetTotalTokens())
	})

	requireEventually(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		spanResp, err := queryClient.GetSpan(ctx, &tracingv1.GetSpanRequest{TraceId: traceID, SpanId: spanID})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		span := singleSpan(c, spanResp.GetResourceSpans())
		if span == nil {
			return
		}
		assertStringAttr(c, span.GetAttributes(), attrLLMSystem, defaultLLMSystem)
		assertStringAttr(c, span.GetAttributes(), attrLLMRequestModel, defaultLLMModel)
		assertStringAttr(c, span.GetAttributes(), attrLLMFinishReason, defaultLLMFinishReason)
		assertStringAttr(c, span.GetAttributes(), attrLLMResponseText, defaultLLMResponseText)
		assertIntAttr(c, span.GetAttributes(), attrUsageInputTokens, 100)
		assertIntAttr(c, span.GetAttributes(), attrUsageOutputTokens, 50)
		assertIntAttr(c, span.GetAttributes(), attrUsageCacheReadInputTokens, 10)
		assertIntAttr(c, span.GetAttributes(), attrUsageReasoningTokens, 5)
	})
}

func TestIngestToolExecutionSpan(t *testing.T) {
	traceID := newTraceID()
	spanID := newSpanID()
	threadID := fmt.Sprintf("thread-%x", traceID)
	startNs := uint64(time.Now().UnixNano())
	span := buildSpan(
		spanToolExecution,
		traceID,
		spanID,
		startNs,
		startNs+3_000_000,
		toolExecutionAttrs(defaultToolName, `{"command":"ls"}`, "file1.txt\nfile2.txt", "call-123"),
	)
	resourceSpans := buildResourceSpans([]*tracev1.Span{span}, resourceAttrs(threadID))

	collectorClient, queryClient := newTracingClients(t)
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	resp, err := exportSpans(ctx, collectorClient, []*tracev1.ResourceSpans{resourceSpans})
	require.NoError(t, err)
	require.Equal(t, int64(0), resp.GetPartialSuccess().GetRejectedSpans())

	requireEventually(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		spanResp, err := queryClient.GetSpan(ctx, &tracingv1.GetSpanRequest{TraceId: traceID, SpanId: spanID})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		span := singleSpan(c, spanResp.GetResourceSpans())
		if span == nil {
			return
		}
		assertStringAttr(c, span.GetAttributes(), attrToolName, defaultToolName)
		assertStringAttr(c, span.GetAttributes(), attrToolInput, `{"command":"ls"}`)
		assertStringAttr(c, span.GetAttributes(), attrToolOutput, "file1.txt\nfile2.txt")
		assertStringAttr(c, span.GetAttributes(), attrToolCallID, "call-123")
	})

	requireEventually(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		listResp, err := queryClient.ListSpans(ctx, &tracingv1.ListSpansRequest{
			OrganizationId: defaultOrganizationID,
			Filter: &tracingv1.SpanFilter{
				TraceId: traceID,
				Names:   []string{spanToolExecution},
			},
		})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		spans := flattenSpans(listResp.GetResourceSpans())
		if !assert.Len(c, spans, 1) {
			return
		}
		assert.Equal(c, spanToolExecution, spans[0].GetName())
	})
}

func TestIngestSummarizationSpan(t *testing.T) {
	traceID := newTraceID()
	spanID := newSpanID()
	threadID := fmt.Sprintf("thread-%x", traceID)
	startNs := uint64(time.Now().UnixNano())
	span := buildSpan(
		spanSummarization,
		traceID,
		spanID,
		startNs,
		startNs+2_000_000,
		summarizationAttrs(defaultSummarizationText, 3, 1500),
	)
	resourceSpans := buildResourceSpans([]*tracev1.Span{span}, resourceAttrs(threadID))

	collectorClient, queryClient := newTracingClients(t)
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	resp, err := exportSpans(ctx, collectorClient, []*tracev1.ResourceSpans{resourceSpans})
	require.NoError(t, err)
	require.Equal(t, int64(0), resp.GetPartialSuccess().GetRejectedSpans())

	requireEventually(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		spanResp, err := queryClient.GetSpan(ctx, &tracingv1.GetSpanRequest{TraceId: traceID, SpanId: spanID})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		span := singleSpan(c, spanResp.GetResourceSpans())
		if span == nil {
			return
		}
		assertStringAttr(c, span.GetAttributes(), attrSummarizationText, defaultSummarizationText)
		assertIntAttr(c, span.GetAttributes(), attrSummarizationNewContext, 3)
		assertIntAttr(c, span.GetAttributes(), attrSummarizationOldContext, 1500)
	})
}

func TestFullAgentTrace(t *testing.T) {
	traceID := newTraceID()
	threadID := fmt.Sprintf("thread-%x", traceID)
	startNs := uint64(time.Now().UnixNano())
	invocationSpan := buildSpan(
		spanInvocationMessage,
		traceID,
		newSpanID(),
		startNs,
		startNs+1_000_000,
		invocationMessageAttrs("Hello from agent"),
	)
	llmSpan := buildSpan(
		spanLLMCall,
		traceID,
		newSpanID(),
		startNs+2_000_000,
		startNs+4_000_000,
		llmCallAttrs(defaultLLMResponseText, 42, 21, 0, 0),
	)
	resourceSpans := buildResourceSpans([]*tracev1.Span{invocationSpan, llmSpan}, resourceAttrs(threadID))

	collectorClient, queryClient := newTracingClients(t)
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	resp, err := exportSpans(ctx, collectorClient, []*tracev1.ResourceSpans{resourceSpans})
	require.NoError(t, err)
	require.Equal(t, int64(0), resp.GetPartialSuccess().GetRejectedSpans())

	requireEventually(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		summary, err := queryClient.GetTraceSummary(ctx, &tracingv1.GetTraceSummaryRequest{TraceId: traceID})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		assert.Equal(c, int64(2), summary.GetTotalSpans())
		assert.Equal(c, int64(1), summary.GetCountsByName()[spanInvocationMessage])
		assert.Equal(c, int64(1), summary.GetCountsByName()[spanLLMCall])
		assert.Equal(c, tracingv1.TraceStatus_TRACE_STATUS_COMPLETED, summary.GetStatus())
	})
}

func TestMCPToolTrace(t *testing.T) {
	traceID := newTraceID()
	threadID := fmt.Sprintf("thread-%x", traceID)
	startNs := uint64(time.Now().UnixNano())
	spans := []*tracev1.Span{
		buildSpan(
			spanInvocationMessage,
			traceID,
			newSpanID(),
			startNs,
			startNs+1_000_000,
			invocationMessageAttrs("Use MCP tools"),
		),
		buildSpan(
			spanLLMCall,
			traceID,
			newSpanID(),
			startNs+2_000_000,
			startNs+3_000_000,
			llmCallAttrs(defaultLLMToolCallResponseText, 12, 4, 0, 0),
		),
		buildSpan(
			spanToolExecution,
			traceID,
			newSpanID(),
			startNs+4_000_000,
			startNs+5_000_000,
			toolExecutionAttrs("mcp.list", "{}", "[]", "call-1"),
		),
		buildSpan(
			spanToolExecution,
			traceID,
			newSpanID(),
			startNs+6_000_000,
			startNs+7_000_000,
			toolExecutionAttrs("mcp.read", `{"path":"/tmp/file"}`, "content", "call-2"),
		),
		buildSpan(
			spanLLMCall,
			traceID,
			newSpanID(),
			startNs+8_000_000,
			startNs+9_000_000,
			llmCallAttrs("Final answer", 20, 8, 0, 0),
		),
	}
	resourceSpans := buildResourceSpans(spans, resourceAttrs(threadID))

	collectorClient, queryClient := newTracingClients(t)
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	resp, err := exportSpans(ctx, collectorClient, []*tracev1.ResourceSpans{resourceSpans})
	require.NoError(t, err)
	require.Equal(t, int64(0), resp.GetPartialSuccess().GetRejectedSpans())

	expectedNames := []string{
		spanInvocationMessage,
		spanLLMCall,
		spanToolExecution,
		spanToolExecution,
		spanLLMCall,
	}

	requireEventually(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		summary, err := queryClient.GetTraceSummary(ctx, &tracingv1.GetTraceSummaryRequest{TraceId: traceID})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		assert.Equal(c, int64(5), summary.GetTotalSpans())
		assert.Equal(c, int64(1), summary.GetCountsByName()[spanInvocationMessage])
		assert.Equal(c, int64(2), summary.GetCountsByName()[spanLLMCall])
		assert.Equal(c, int64(2), summary.GetCountsByName()[spanToolExecution])
	})

	requireEventually(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		listResp, err := queryClient.ListSpans(ctx, &tracingv1.ListSpansRequest{
			OrganizationId: defaultOrganizationID,
			Filter:         &tracingv1.SpanFilter{TraceId: traceID},
			OrderBy:        tracingv1.ListSpansOrderBy_LIST_SPANS_ORDER_BY_START_TIME_ASC,
		})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		spans := flattenSpans(listResp.GetResourceSpans())
		if !assert.Len(c, spans, len(expectedNames)) {
			return
		}
		assert.Equal(c, expectedNames, spanNames(spans))
	})
}

func TestSummarizationTrace(t *testing.T) {
	traceID := newTraceID()
	threadID := fmt.Sprintf("thread-%x", traceID)
	startNs := uint64(time.Now().UnixNano())
	summarizationSpanID := newSpanID()
	spans := []*tracev1.Span{
		buildSpan(
			spanInvocationMessage,
			traceID,
			newSpanID(),
			startNs,
			startNs+1_000_000,
			invocationMessageAttrs("Summarize this"),
		),
		buildSpan(
			spanSummarization,
			traceID,
			summarizationSpanID,
			startNs+2_000_000,
			startNs+3_000_000,
			summarizationAttrs("Short summary", 2, 900),
		),
		buildSpan(
			spanLLMCall,
			traceID,
			newSpanID(),
			startNs+4_000_000,
			startNs+5_000_000,
			llmCallAttrs(defaultLLMResponseText, 60, 30, 0, 0),
		),
	}
	resourceSpans := buildResourceSpans(spans, resourceAttrs(threadID))

	collectorClient, queryClient := newTracingClients(t)
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	resp, err := exportSpans(ctx, collectorClient, []*tracev1.ResourceSpans{resourceSpans})
	require.NoError(t, err)
	require.Equal(t, int64(0), resp.GetPartialSuccess().GetRejectedSpans())

	requireEventually(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		spanResp, err := queryClient.GetSpan(ctx, &tracingv1.GetSpanRequest{TraceId: traceID, SpanId: summarizationSpanID})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		span := singleSpan(c, spanResp.GetResourceSpans())
		if span == nil {
			return
		}
		assertStringAttr(c, span.GetAttributes(), attrSummarizationText, "Short summary")
		assertIntAttr(c, span.GetAttributes(), attrSummarizationNewContext, 2)
		assertIntAttr(c, span.GetAttributes(), attrSummarizationOldContext, 900)
	})

	requireEventually(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		summary, err := queryClient.GetTraceSummary(ctx, &tracingv1.GetTraceSummaryRequest{TraceId: traceID})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		assert.Equal(c, int64(3), summary.GetTotalSpans())
		assert.Equal(c, int64(1), summary.GetCountsByName()[spanInvocationMessage])
		assert.Equal(c, int64(1), summary.GetCountsByName()[spanSummarization])
		assert.Equal(c, int64(1), summary.GetCountsByName()[spanLLMCall])
	})
}

func TestUpsertInProgressSpan(t *testing.T) {
	traceID := newTraceID()
	spanID := newSpanID()
	threadID := fmt.Sprintf("thread-%x", traceID)
	startNs := uint64(time.Now().UnixNano())

	collectorClient, queryClient := newTracingClients(t)
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	firstSpan := buildSpan(
		spanLLMCall,
		traceID,
		spanID,
		startNs,
		0,
		llmCallAttrs("Working...", 10, 5, 0, 0),
	)
	resp, err := exportSpans(ctx, collectorClient, []*tracev1.ResourceSpans{
		buildResourceSpans([]*tracev1.Span{firstSpan}, resourceAttrs(threadID)),
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), resp.GetPartialSuccess().GetRejectedSpans())

	requireEventually(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		spanResp, err := queryClient.GetSpan(ctx, &tracingv1.GetSpanRequest{TraceId: traceID, SpanId: spanID})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		span := singleSpan(c, spanResp.GetResourceSpans())
		if span == nil {
			return
		}
		assert.Equal(c, uint64(0), span.GetEndTimeUnixNano())
	})

	requireEventually(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		listResp, err := queryClient.ListSpans(ctx, &tracingv1.ListSpansRequest{
			OrganizationId: defaultOrganizationID,
			Filter: &tracingv1.SpanFilter{
				TraceId:  traceID,
				Statuses: []tracingv1.SpanStatus{tracingv1.SpanStatus_SPAN_STATUS_RUNNING},
			},
		})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		spans := flattenSpans(listResp.GetResourceSpans())
		if !assert.Len(c, spans, 1) {
			return
		}
		assert.Equal(c, spanID, spans[0].GetSpanId())
	})

	ctx, cancel = context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	updatedSpan := buildSpan(
		spanLLMCall,
		traceID,
		spanID,
		startNs,
		startNs+5_000_000,
		llmCallAttrs("Done", 10, 5, 0, 0),
	)
	resp, err = exportSpans(ctx, collectorClient, []*tracev1.ResourceSpans{
		buildResourceSpans([]*tracev1.Span{updatedSpan}, resourceAttrs(threadID)),
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), resp.GetPartialSuccess().GetRejectedSpans())

	requireEventually(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		spanResp, err := queryClient.GetSpan(ctx, &tracingv1.GetSpanRequest{TraceId: traceID, SpanId: spanID})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		span := singleSpan(c, spanResp.GetResourceSpans())
		if span == nil {
			return
		}
		assert.Greater(c, span.GetEndTimeUnixNano(), uint64(0))
	})

	requireEventually(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		listResp, err := queryClient.ListSpans(ctx, &tracingv1.ListSpansRequest{
			OrganizationId: defaultOrganizationID,
			Filter: &tracingv1.SpanFilter{
				TraceId:  traceID,
				Statuses: []tracingv1.SpanStatus{tracingv1.SpanStatus_SPAN_STATUS_OK},
			},
		})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		spans := flattenSpans(listResp.GetResourceSpans())
		if !assert.Len(c, spans, 1) {
			return
		}
		assert.Equal(c, spanID, spans[0].GetSpanId())
	})
}

func TestListSpansFiltering(t *testing.T) {
	traceID := newTraceID()
	threadID := fmt.Sprintf("thread-%x", traceID)
	startNs := uint64(time.Now().UnixNano())
	spans := []*tracev1.Span{
		buildSpan(
			spanInvocationMessage,
			traceID,
			newSpanID(),
			startNs,
			startNs+1_000_000,
			invocationMessageAttrs("Filter spans"),
		),
		buildSpan(
			spanLLMCall,
			traceID,
			newSpanID(),
			startNs+2_000_000,
			startNs+3_000_000,
			llmCallAttrs("Filter response", 20, 10, 0, 0),
		),
		buildSpan(
			spanToolExecution,
			traceID,
			newSpanID(),
			startNs+4_000_000,
			startNs+5_000_000,
			toolExecutionAttrs("tool.error", "{}", "failure", "call-error"),
			withStatus(tracev1.Status_STATUS_CODE_ERROR, "tool failed"),
		),
		buildSpan(
			spanSummarization,
			traceID,
			newSpanID(),
			startNs+6_000_000,
			startNs+7_000_000,
			summarizationAttrs("Filter summary", 1, 200),
		),
	}
	resourceSpans := buildResourceSpans(spans, resourceAttrs(threadID))

	collectorClient, queryClient := newTracingClients(t)
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	resp, err := exportSpans(ctx, collectorClient, []*tracev1.ResourceSpans{resourceSpans})
	require.NoError(t, err)
	require.Equal(t, int64(0), resp.GetPartialSuccess().GetRejectedSpans())

	requireEventually(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		listResp, err := queryClient.ListSpans(ctx, &tracingv1.ListSpansRequest{
			OrganizationId: defaultOrganizationID,
			Filter: &tracingv1.SpanFilter{
				TraceId: traceID,
				Names:   []string{spanLLMCall},
			},
		})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		spans := flattenSpans(listResp.GetResourceSpans())
		if !assert.Len(c, spans, 1) {
			return
		}
		assert.Equal(c, spanLLMCall, spans[0].GetName())
	})

	requireEventually(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		listResp, err := queryClient.ListSpans(ctx, &tracingv1.ListSpansRequest{
			OrganizationId: defaultOrganizationID,
			Filter: &tracingv1.SpanFilter{
				TraceId:  traceID,
				Statuses: []tracingv1.SpanStatus{tracingv1.SpanStatus_SPAN_STATUS_ERROR},
			},
		})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		spans := flattenSpans(listResp.GetResourceSpans())
		if !assert.Len(c, spans, 1) {
			return
		}
		assert.Equal(c, spanToolExecution, spans[0].GetName())
	})

	requireEventually(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		listResp, err := queryClient.ListSpans(ctx, &tracingv1.ListSpansRequest{
			OrganizationId: defaultOrganizationID,
			Filter:         &tracingv1.SpanFilter{TraceId: traceID},
			PageSize:       2,
			OrderBy:        tracingv1.ListSpansOrderBy_LIST_SPANS_ORDER_BY_START_TIME_ASC,
		})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		spans := flattenSpans(listResp.GetResourceSpans())
		if !assert.Len(c, spans, 2) {
			return
		}
		assert.NotEmpty(c, listResp.GetNextPageToken())
		ctxNext, cancelNext := context.WithTimeout(context.Background(), requestTimeout)
		defer cancelNext()
		secondResp, err := queryClient.ListSpans(ctxNext, &tracingv1.ListSpansRequest{
			OrganizationId: defaultOrganizationID,
			Filter:         &tracingv1.SpanFilter{TraceId: traceID},
			PageSize:       2,
			OrderBy:        tracingv1.ListSpansOrderBy_LIST_SPANS_ORDER_BY_START_TIME_ASC,
			PageToken:      listResp.GetNextPageToken(),
		})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		secondSpans := flattenSpans(secondResp.GetResourceSpans())
		if !assert.Len(c, secondSpans, 2) {
			return
		}
		assert.Empty(c, secondResp.GetNextPageToken())
	})
}

func TestListSpansOrganizationAndMessageFilters(t *testing.T) {
	traceID := newTraceID()
	threadID := fmt.Sprintf("thread-%x", traceID)
	startNs := uint64(time.Now().UnixNano())
	orgSpan := buildSpan(
		spanInvocationMessage,
		traceID,
		newSpanID(),
		startNs,
		startNs+1_000_000,
		invocationMessageAttrs("Org scope"),
	)
	messageSpan := buildSpan(
		spanLLMCall,
		traceID,
		newSpanID(),
		startNs+2_000_000,
		startNs+3_000_000,
		llmCallAttrs("Message filter", 5, 2, 0, 0),
	)
	otherOrgSpan := buildSpan(
		spanToolExecution,
		traceID,
		newSpanID(),
		startNs+4_000_000,
		startNs+5_000_000,
		toolExecutionAttrs("org.tool", "{}", "ok", "call-org"),
	)
	resourceSpans := []*tracev1.ResourceSpans{
		buildResourceSpans([]*tracev1.Span{orgSpan}, resourceAttrsWithMessageID(threadID, defaultOrganizationID, defaultMessageID)),
		buildResourceSpans([]*tracev1.Span{messageSpan}, resourceAttrsWithMessageID(threadID, defaultOrganizationID, otherMessageID)),
		buildResourceSpans([]*tracev1.Span{otherOrgSpan}, resourceAttrsWithMessageID(threadID, otherOrganizationID, defaultMessageID)),
	}

	collectorClient, queryClient := newTracingClients(t)
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	resp, err := exportSpans(ctx, collectorClient, resourceSpans)
	require.NoError(t, err)
	require.Equal(t, int64(0), resp.GetPartialSuccess().GetRejectedSpans())

	requireEventually(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		listResp, err := queryClient.ListSpans(ctx, &tracingv1.ListSpansRequest{
			OrganizationId: defaultOrganizationID,
			Filter:         &tracingv1.SpanFilter{TraceId: traceID},
		})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		spans := flattenSpans(listResp.GetResourceSpans())
		if !assert.Len(c, spans, 2) {
			return
		}
		assert.ElementsMatch(c, []string{spanInvocationMessage, spanLLMCall}, spanNames(spans))
	})

	requireEventually(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		listResp, err := queryClient.ListSpans(ctx, &tracingv1.ListSpansRequest{
			OrganizationId: defaultOrganizationID,
			Filter: &tracingv1.SpanFilter{
				TraceId:   traceID,
				MessageId: defaultMessageID,
			},
		})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		spans := flattenSpans(listResp.GetResourceSpans())
		if !assert.Len(c, spans, 1) {
			return
		}
		assert.Equal(c, spanInvocationMessage, spans[0].GetName())
	})

	requireEventually(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		listResp, err := queryClient.ListSpans(ctx, &tracingv1.ListSpansRequest{
			OrganizationId: defaultOrganizationID,
			Filter: &tracingv1.SpanFilter{
				TraceId:   traceID,
				MessageId: missingMessageID,
			},
		})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		spans := flattenSpans(listResp.GetResourceSpans())
		assert.Empty(c, spans)
	})
}

func TestLLMContextEvents(t *testing.T) {
	traceID := newTraceID()
	spanID := newSpanID()
	threadID := fmt.Sprintf("thread-%x", traceID)
	startNs := uint64(time.Now().UnixNano())
	span := buildSpan(
		spanLLMCall,
		traceID,
		spanID,
		startNs,
		startNs+3_000_000,
		llmCallAttrs(defaultLLMResponseText, 30, 20, 0, 0),
		withEvents(
			contextEvent("user", "Context 1", true, 120, startNs+500_000),
			contextEvent("assistant", "Context 2", false, 240, startNs+1_000_000),
			contextEvent("user", "Context 3", true, 360, startNs+1_500_000),
		),
	)
	resourceSpans := buildResourceSpans([]*tracev1.Span{span}, resourceAttrs(threadID))

	collectorClient, queryClient := newTracingClients(t)
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	resp, err := exportSpans(ctx, collectorClient, []*tracev1.ResourceSpans{resourceSpans})
	require.NoError(t, err)
	require.Equal(t, int64(0), resp.GetPartialSuccess().GetRejectedSpans())

	requireEventually(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		spanResp, err := queryClient.GetSpan(ctx, &tracingv1.GetSpanRequest{TraceId: traceID, SpanId: spanID})
		assert.NoError(c, err)
		if err != nil {
			return
		}
		span := singleSpan(c, spanResp.GetResourceSpans())
		if span == nil {
			return
		}
		events := span.GetEvents()
		if !assert.Len(c, events, 3) {
			return
		}
		assert.Equal(c, eventLLMContextItem, events[0].GetName())
		assertStringAttr(c, events[0].GetAttributes(), attrContextRole, "user")
		assertStringAttr(c, events[0].GetAttributes(), attrContextText, "Context 1")
		assertStringAttr(c, events[0].GetAttributes(), attrContextIsNew, "true")
		assertIntAttr(c, events[0].GetAttributes(), attrContextSizeBytes, 120)

		assert.Equal(c, eventLLMContextItem, events[1].GetName())
		assertStringAttr(c, events[1].GetAttributes(), attrContextRole, "assistant")
		assertStringAttr(c, events[1].GetAttributes(), attrContextText, "Context 2")
		assertStringAttr(c, events[1].GetAttributes(), attrContextIsNew, "false")
		assertIntAttr(c, events[1].GetAttributes(), attrContextSizeBytes, 240)

		assert.Equal(c, eventLLMContextItem, events[2].GetName())
		assertStringAttr(c, events[2].GetAttributes(), attrContextRole, "user")
		assertStringAttr(c, events[2].GetAttributes(), attrContextText, "Context 3")
		assertStringAttr(c, events[2].GetAttributes(), attrContextIsNew, "true")
		assertIntAttr(c, events[2].GetAttributes(), attrContextSizeBytes, 360)
	})
}

func invocationMessageAttrs(text string) []*commonv1.KeyValue {
	return []*commonv1.KeyValue{
		stringAttr(attrMessageText, text),
		stringAttr(attrMessageRole, defaultInvocationMessageRole),
		stringAttr(attrMessageKind, defaultInvocationMessageKind),
	}
}

func llmCallAttrs(responseText string, inputTokens, outputTokens, cacheReadTokens, reasoningTokens int64) []*commonv1.KeyValue {
	return []*commonv1.KeyValue{
		stringAttr(attrLLMSystem, defaultLLMSystem),
		stringAttr(attrLLMRequestModel, defaultLLMModel),
		stringAttr(attrLLMFinishReason, defaultLLMFinishReason),
		stringAttr(attrLLMResponseText, responseText),
		intAttr(attrUsageInputTokens, inputTokens),
		intAttr(attrUsageOutputTokens, outputTokens),
		intAttr(attrUsageCacheReadInputTokens, cacheReadTokens),
		intAttr(attrUsageReasoningTokens, reasoningTokens),
	}
}

func toolExecutionAttrs(name, input, output, callID string) []*commonv1.KeyValue {
	return []*commonv1.KeyValue{
		stringAttr(attrToolName, name),
		stringAttr(attrToolInput, input),
		stringAttr(attrToolOutput, output),
		stringAttr(attrToolCallID, callID),
	}
}

func summarizationAttrs(text string, newContextCount, oldContextTokens int64) []*commonv1.KeyValue {
	return []*commonv1.KeyValue{
		stringAttr(attrSummarizationText, text),
		intAttr(attrSummarizationNewContext, newContextCount),
		intAttr(attrSummarizationOldContext, oldContextTokens),
	}
}

func resourceAttrs(threadID string) []*commonv1.KeyValue {
	return resourceAttrsWithMessageID(threadID, defaultOrganizationID, "")
}

func resourceAttrsWithMessageID(threadID, organizationID, messageID string) []*commonv1.KeyValue {
	attrs := []*commonv1.KeyValue{
		stringAttr(attrThreadID, threadID),
		stringAttr(attrOrganizationID, organizationID),
	}
	if messageID != "" {
		attrs = append(attrs, stringAttr(attrMessageID, messageID))
	}
	return attrs
}

func contextEvent(role, text string, isNew bool, sizeBytes int64, timestampNs uint64) *tracev1.Span_Event {
	isNewValue := "false"
	if isNew {
		isNewValue = "true"
	}
	return &tracev1.Span_Event{
		Name:         eventLLMContextItem,
		TimeUnixNano: timestampNs,
		Attributes: []*commonv1.KeyValue{
			stringAttr(attrContextRole, role),
			stringAttr(attrContextText, text),
			stringAttr(attrContextIsNew, isNewValue),
			intAttr(attrContextSizeBytes, sizeBytes),
		},
	}
}
