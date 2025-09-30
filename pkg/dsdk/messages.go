package dsdk

import (
	"encoding/json"
	"fmt"

	"github.com/go-playground/validator/v10"
)

var validate = validator.New()

type DataFlowBaseMessage struct {
	MessageID              string       `json:"messageID" validate:"required"`
	ParticipantID          string       `json:"participantID" validate:"required"`
	CounterPartyID         string       `json:"counterPartyID" validate:"required"`
	DataspaceContext       string       `json:"dataspaceContext" validate:"required"`
	ProcessID              string       `json:"processID" validate:"required"`
	AgreementID            string       `json:"agreementID" validate:"required"`
	DatasetID              string       `json:"datasetID" validate:"required"`
	CallbackAddress        CallbackURL  `json:"callbackAddress" validate:"required"`
	TransferType           TransferType `json:"transferType" validate:"required"`
	DestinationDataAddress DataAddress  `json:"destinationDataAddress" validate:"required"`
}

func (m *DataFlowBaseMessage) UnmarshalJSON(data []byte) error {
	type Alias DataFlowBaseMessage
	aux := &Alias{}

	if err := json.Unmarshal(data, aux); err != nil {
		return fmt.Errorf("error deserializing message: %w, %w", err, ErrInvalidInput)
	}

	// validate the unmarshalled struct
	if err := validate.Struct(aux); err != nil {
		// You can format errors however you want
		return NewValidationError("validation failed: %w", err.Error())
	}

	if aux.CallbackAddress.IsEmpty() {
		return NewValidationError("callback address is required")
	}

	*m = DataFlowBaseMessage(*aux)
	return nil
}

type DataFlowStartMessage struct {
	DataFlowBaseMessage
	SourceDataAddress *DataAddress `json:"sourceDataAddress,omitempty" validate:"required"`
}

func (m *DataFlowStartMessage) UnmarshalJSON(data []byte) error {
	type Alias DataFlowStartMessage
	aux := &Alias{}

	if err := json.Unmarshal(data, aux); err != nil {
		return fmt.Errorf("error deserializing message: %w, %w", err, ErrInvalidInput)
	}

	if err := validate.Struct(aux); err != nil {
		return NewValidationError("validation failed: %w", err.Error())
	}

	*m = DataFlowStartMessage(*aux)
	return nil
}

type DataFlowPrepareMessage struct {
	DataFlowBaseMessage
}

type DataFlowTransitionMessage struct {
	Reason string `json:"reason"`
}
type DataFlowResponseMessage struct {
	DataplaneID string        `json:"dataplaneID"`
	DataAddress *DataAddress  `json:"dataAddress,omitempty"`
	State       DataFlowState `json:"state"`
	Error       string        `json:"error"`
}

type DataFlowStatusResponseMessage struct {
	State      DataFlowState `json:"state"`
	DataFlowID string        `json:"dataFlowID"`
}
