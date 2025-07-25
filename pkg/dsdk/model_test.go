package dsdk

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"testing"
	"time"
)

func Test_dataFlowStartSerialize(t *testing.T) {
	callbackURL, _ := url.Parse("https://example.com/callback")
	build, _ := NewDataAddressBuilder().Property("foo", "bar").Build()
	original := DataFlowStartMessage{
		DataFlowBaseMessage: DataFlowBaseMessage{
			ParticipantId:   "participant123",
			AgreementId:     "agreement456",
			CallbackAddress: CallbackURL(*callbackURL),
			TransferType: TransferType{
				DestinationType: "PULL",
				FlowType:        FlowType("PULL"),
			},
			DestinationDataAddress: *build,
		},
		SourceDataAddress: *build,
	}

	jsonData, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded DataFlowStartMessage
	err = json.Unmarshal(jsonData, &decoded)
	if err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	var errs []error

	if decoded.ParticipantId != original.ParticipantId {
		errs = append(errs, fmt.Errorf("invalid ParticipantId"))
	}

	if decoded.AgreementId != original.AgreementId {
		errs = append(errs, fmt.Errorf("invalid AgreementId"))
	}

	if decoded.CallbackAddress != original.CallbackAddress {
		errs = append(errs, fmt.Errorf("invalid CallbackAddress"))
	}

	if decoded.TransferType != original.TransferType {
		errs = append(errs, fmt.Errorf("invalid TransferType"))
	}

	if testErr := errors.Join(errs...); testErr != nil {
		t.Error(testErr)
	}
}

func TestDataFlowBuilder_Build(t *testing.T) {
	validURL, err := url.Parse("http://example.com/callback")
	if err != nil {
		t.Fatal("failed to parse test URL:", err)
	}

	tests := map[string]struct {
		builder func() *DataFlowBuilder
		wantErr bool
	}{
		"success with all fields": {
			builder: func() *DataFlowBuilder {
				return NewDataFlowBuilder().
					ID("test-id").
					UpdatedAt(int64(time.Now().Unix())).
					CreatedAt(int64(time.Now().Unix())).
					ParticipantId("part-123").
					DataspaceContext("ctx-123").
					CounterpartyId("counter-123").
					State(Started).
					StateTimestamp(time.Now().Unix()).
					SourceDataAddress(DataAddress{Properties: map[string]any{"source": "test"}}).
					DestinationDataAddress(DataAddress{Properties: map[string]any{"dest": "test"}}).
					CallbackAddress(CallbackURL(*validURL)).
					TransferType(TransferType{
						DestinationType: "test-dest",
						FlowType:        Pull,
					}).
					RuntimeId("runtime-123")
			},
			wantErr: false,
		},
		"fails with empty builder": {
			builder: func() *DataFlowBuilder {
				return NewDataFlowBuilder()
			},
			wantErr: true,
		},
		"fails without ID": {
			builder: func() *DataFlowBuilder {
				b := createValidBuilder(validURL)
				return b.ID("")
			},
			wantErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			flow, err := tc.builder().Build()

			if tc.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Verify non-zero/non-empty values for required fields
			if flow.ID == "" {
				t.Error("ID is empty")
			}
			if flow.UpdatedAt == 0 {
				t.Error("UpdatedAt is zero")
			}
			if flow.CreatedAt == 0 {
				t.Error("CreatedAt is zero")
			}
			if flow.ParticipantId == "" {
				t.Error("ParticipantId is empty")
			}
			if flow.DataspaceContext == "" {
				t.Error("DataspaceContext is empty")
			}
			if flow.CounterPartyId == "" {
				t.Error("CounterPartyId is empty")
			}
			if flow.StateTimestamp == 0 {
				t.Error("StateTimestamp is zero")
			}
			if flow.CallbackAddress.URL() == nil {
				t.Error("CallbackAddress is nil")
			}
			if flow.TransferType.DestinationType == "" {
				t.Error("TransferType destination type is empty")
			}
			if flow.TransferType.FlowType == "" {
				t.Error("TransferType flow type is empty")
			}
		})
	}
}

// Helper function to create a valid builder for testing
func createValidBuilder(validURL *url.URL) *DataFlowBuilder {
	return NewDataFlowBuilder().
		ID("test-id").
		UpdatedAt(int64(time.Now().Unix())).
		CreatedAt(int64(time.Now().Unix())).
		ParticipantId("part-123").
		DataspaceContext("ctx-123").
		CounterpartyId("counter-123").
		State(Started).
		StateTimestamp(time.Now().Unix()).
		SourceDataAddress(DataAddress{Properties: map[string]any{"source": "test"}}).
		DestinationDataAddress(DataAddress{Properties: map[string]any{"dest": "test"}}).
		CallbackAddress(CallbackURL(*validURL)).
		TransferType(TransferType{
			DestinationType: "test-dest",
			FlowType:        Pull,
		}).
		RuntimeId("runtime-123")
}
