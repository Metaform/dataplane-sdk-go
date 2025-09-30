package dsdk

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_Message_InvalidJson(t *testing.T) {
	// counterPartyID should be a string, not an object:
	payload := `{
					"messageID": "test-id",
					"participantID": "test-id",
					"counterPartyID": {}
				}`
	msg := DataFlowPrepareMessage{}
	err := msg.UnmarshalJSON([]byte(payload))
	assert.ErrorIs(t, err, ErrInvalidInput)
}

func Test_Message_MissingProperties_Unmarshalling(t *testing.T) {

	payload := `{
				"messageID": "test-id"
			}`
	msg := DataFlowPrepareMessage{}
	err := msg.UnmarshalJSON([]byte(payload))
	assert.ErrorIs(t, err, ErrValidation)
}

func Test_Message_MissingProperties_Decoding(t *testing.T) {
	payload := `{
				"messageID": "test-id"
			}`
	reader := strings.NewReader(payload)

	message := DataFlowBaseMessage{}
	err := json.NewDecoder(reader).Decode(&message)
	assert.ErrorIs(t, err, ErrValidation)
}

func Test_Message_Success(t *testing.T) {
	payload := createMessage()
	serializedPayload, err := json.Marshal(payload)
	assert.NoError(t, err)

	deserialized := DataFlowPrepareMessage{}
	err2 := json.NewDecoder(strings.NewReader(string(serializedPayload))).Decode(&deserialized)
	assert.NoError(t, err2)
}

func Test_Message_InvalidCallbackAddress(t *testing.T) {
	payload := createMessage()
	payload.CallbackAddress = CallbackURL{}
	serializedPayload, err := json.Marshal(payload)
	assert.NoError(t, err)

	deserialized := DataFlowPrepareMessage{}
	err2 := json.NewDecoder(strings.NewReader(string(serializedPayload))).Decode(&deserialized)
	assert.ErrorIs(t, err2, ErrValidation)
}

func Test_Message_InvalidTransferType(t *testing.T) {
	payload := createMessage()
	payload.TransferType = TransferType{}
	serializedPayload, err := json.Marshal(payload)
	assert.NoError(t, err)

	deserialized := DataFlowPrepareMessage{}
	err2 := json.NewDecoder(strings.NewReader(string(serializedPayload))).Decode(&deserialized)
	assert.ErrorIs(t, err2, ErrValidation)
}

func createMessage() DataFlowPrepareMessage {
	return DataFlowPrepareMessage{
		DataFlowBaseMessage: DataFlowBaseMessage{
			MessageID:        uuid.New().String(),
			ParticipantID:    uuid.New().String(),
			CounterPartyID:   uuid.New().String(),
			DataspaceContext: uuid.New().String(),
			ProcessID:        uuid.New().String(),
			AgreementID:      uuid.New().String(),
			DatasetID:        uuid.New().String(),
			CallbackAddress:  CallbackURL{Scheme: "http", Host: "test.com", Path: "/callback"},
			TransferType: TransferType{
				DestinationType: "test-type",
				FlowType:        "pull",
			},
			DestinationDataAddress: DataAddress{},
		},
	}
}
