package rootly

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	pfbridge "github.com/pulumi/pulumi-terraform-bridge/v3/pkg/pf/tfbridge"
	pulumirpc "github.com/pulumi/pulumi/sdk/v3/proto/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

// Regression test for https://github.com/rootlyhq/pulumi-rootly/issues/73: the nested
// alertPayloadConditions block must reach the API on create and update, and round-trip.
func TestWorkflowAlertSendsAlertPayloadConditions(t *testing.T) {
	var mu sync.Mutex
	var writes []map[string]any
	stored := map[string]any{}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/vnd.api+json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/workflows",
			r.Method == http.MethodPut && r.URL.Path == "/v1/workflows/workflow-1":
			raw, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			var doc struct {
				Data struct {
					Attributes map[string]any `json:"attributes"`
				} `json:"data"`
			}
			require.NoError(t, json.Unmarshal(raw, &doc))
			writes = append(writes, doc.Data.Attributes)
			for k, v := range doc.Data.Attributes {
				stored[k] = v
			}
		case r.Method == http.MethodGet && r.URL.Path == "/v1/workflows/workflow-1":
		default:
			t.Errorf("unexpected API request: %s %s", r.Method, r.URL)
			http.NotFound(w, r)
			return
		}
		attrs, err := json.Marshal(stored)
		require.NoError(t, err)
		_, err = fmt.Fprintf(w, `{"data":{"id":"workflow-1","type":"workflows","attributes":%s},"included":[]}`, attrs)
		require.NoError(t, err)
	}))
	defer api.Close()
	ctx := context.Background()
	info, data := testProvider(t)
	server, err := pfbridge.MakeMuxedServer(ctx, "rootly", info, data)(nil)
	require.NoError(t, err)
	args, err := structpb.NewStruct(map[string]any{"apiHost": api.URL, "apiToken": "test-token"})
	require.NoError(t, err)
	_, err = server.Configure(ctx, &pulumirpc.ConfigureRequest{
		Args: args, AcceptSecrets: true, AcceptResources: true,
	})
	require.NoError(t, err)

	urn := "urn:pulumi:test::test::rootly:index/workflowAlert:WorkflowAlert::broadcast"
	inputs := func(logic, operator, query, value string) *structpb.Struct {
		s, err := structpb.NewStruct(map[string]any{
			"name": "broadcast", "enabled": true,
			"triggerParams": map[string]any{
				"triggerType": "alert",
				"triggers":    []any{"alert_created"},
				"alertPayloadConditions": map[string]any{
					"logic": logic,
					"conditions": []any{
						map[string]any{"operator": operator, "query": query, "values": []any{value}},
					},
				},
			},
		})
		require.NoError(t, err)
		return s
	}
	lastWrite := func() any {
		mu.Lock()
		defer mu.Unlock()
		return writes[len(writes)-1]["trigger_params"].(map[string]any)["alert_payload_conditions"]
	}

	checked, err := server.Check(ctx, &pulumirpc.CheckRequest{
		Urn: urn, News: inputs("ALL", "CONTAINS", "$.tags", "env:development"),
	})
	require.NoError(t, err)
	require.Empty(t, checked.Failures)
	created, err := server.Create(ctx, &pulumirpc.CreateRequest{Urn: urn, Properties: checked.Inputs})
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"logic": "ALL",
		"conditions": []any{map[string]any{
			"operator": "CONTAINS", "query": "$.tags", "use_regexp": false, "values": []any{"env:development"},
		}},
	}, lastWrite(), "create must send alert_payload_conditions")
	require.Equal(t, map[string]any{
		"logic": "ALL",
		"conditions": []any{map[string]any{
			"operator": "CONTAINS", "query": "$.tags", "useRegexp": false, "values": []any{"env:development"},
		}},
	}, created.Properties.AsMap()["triggerParams"].(map[string]any)["alertPayloadConditions"])

	checked, err = server.Check(ctx, &pulumirpc.CheckRequest{
		Urn: urn, Olds: created.Properties, News: inputs("ANY", "IS", "$.env", "staging"),
	})
	require.NoError(t, err)
	require.Empty(t, checked.Failures)
	_, err = server.Update(ctx, &pulumirpc.UpdateRequest{
		Urn: urn, Id: created.Id, Olds: created.Properties, News: checked.Inputs,
	})
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"logic": "ANY",
		"conditions": []any{map[string]any{
			"operator": "IS", "query": "$.env", "use_regexp": false, "values": []any{"staging"},
		}},
	}, lastWrite(), "update must send alert_payload_conditions")
}
