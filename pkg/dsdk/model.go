package dsdk

import (
	"encoding/json"
	"fmt"
	"net/url"
	"time"
)

type FlowType string

const (
	Pull               FlowType = "pull"
	Push               FlowType = "push"
	TypeKey                     = "@type"
	DataAddressType             = "DataAddress"
	EndpointKey                 = "endpoint"
	EndpointType                = "endpointType"
	EndpointProperties          = "endpointProperties"
)

type DataAddress struct {
	Properties map[string]any `json:"properties"`
}

func NewDataAddressBuilder() *DataAddressBuilder {
	return &DataAddressBuilder{
		properties: make(map[string]any),
	}
}

type DataAddressBuilder struct {
	properties map[string]any
}

func (b *DataAddressBuilder) Property(key string, value any) *DataAddressBuilder {
	b.properties[key] = value
	return b
}

func (b *DataAddressBuilder) EndpointProperty(key string, typeVal string, value any) *DataAddressBuilder {
	endpointProps := b.properties[EndpointProperties]
	if endpointProps == nil {
		endpointProps = make([]any, 0)
		b.properties[EndpointProperties] = endpointProps
	}
	if endpoints, ok := endpointProps.([]any); ok {
		props := map[string]any{
			"key":   key,
			"type":  typeVal,
			"value": value,
		}
		endpoints = append(endpoints, props)
		b.properties[EndpointProperties] = endpoints
	} else {
		panic("endpoint properties is not an array")
	}
	return b
}

func (b *DataAddressBuilder) Properties(props map[string]any) *DataAddressBuilder {
	for k, v := range props {
		b.properties[k] = v
	}
	return b
}

func (b *DataAddressBuilder) Build() (*DataAddress, error) {
	if b.properties[TypeKey] == nil {
		b.properties[TypeKey] = DataAddressType
	}

	return &DataAddress{
		Properties: b.properties,
	}, nil
}

type TransferType struct {
	DestinationType string   `json:"destinationType"`
	FlowType        FlowType `json:"flowType"`
}

type DataFlowBaseMessage struct {
	MessageID              string       `json:"messageID"` // NEW
	ParticipantID          string       `json:"participantID"`
	CounterPartyID         string       `json:"counterPartyID"`
	DataspaceContext       string       `json:"dataspaceContext"`
	ProcessID              string       `json:"processID"`
	AgreementID            string       `json:"agreementID"`
	DatasetID              string       `json:"datasetID"`
	CallbackAddress        CallbackURL  `json:"callbackAddress"`
	TransferType           TransferType `json:"transferType"`
	DestinationDataAddress DataAddress  `json:"destinationDataAddress"`
}

type DataFlowStartMessage struct {
	DataFlowBaseMessage
	SourceDataAddress *DataAddress `json:"sourceDataAddress,omitempty"`
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

type DataFlowState int

func (s DataFlowState) String() string {
	switch s {
	case Uninitialized:
		return "UNINITIALIZED"
	case Preparing:
		return "PREPARING"
	case Prepared:
		return "PREPARED"
	case Starting:
		return "STARTING"
	case Started:
		return "STARTED"
	case Completed:
		return "COMPLETED"
	case Suspended:
		return "SUSPENDED"
	case Terminated:
		return "TERMINATED"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int(s))
	}
}

const (
	Uninitialized DataFlowState = 0
	Preparing     DataFlowState = 50
	Prepared      DataFlowState = 100
	Starting      DataFlowState = 150
	Started       DataFlowState = 200
	Completed     DataFlowState = 250
	Suspended     DataFlowState = 300
	Terminated    DataFlowState = 350
)

type DataFlow struct {
	ID                     string
	Version                int64
	Consumer               bool
	AgreementID            string
	DatasetID              string
	RuntimeID              string
	UpdatedAt              int64
	CreatedAt              int64
	ParticipantID          string
	DataspaceContext       string
	CounterPartyID         string
	CallbackAddress        CallbackURL
	TransferType           TransferType
	State                  DataFlowState
	StateCount             uint
	StateTimestamp         int64
	SourceDataAddress      DataAddress
	DestinationDataAddress DataAddress
	ErrorDetail            string
}

func (df *DataFlow) TransitionToPreparing() error {
	if df.State == Preparing {
		return nil
	}
	if df.State != Uninitialized {
		return fmt.Errorf("invalid transition: cannot transition from %v to PREPARING", df.State)
	}
	df.State = Preparing
	df.StateTimestamp = time.Now().UnixMilli()
	df.StateCount++
	return nil
}

func (df *DataFlow) TransitionToPrepared() error {
	if df.State == Prepared {
		return nil
	}
	if df.State != Uninitialized && df.State != Preparing {
		return fmt.Errorf("invalid transition: cannot transition from %v to PREPARED", df.State)
	}
	df.State = Prepared
	df.StateTimestamp = time.Now().UnixMilli()
	df.StateCount++
	return nil
}

func (df *DataFlow) TransitionToStarting() error {
	if df.State == Starting {
		return nil
	}
	if df.State != Uninitialized && df.State != Prepared {
		return fmt.Errorf("invalid transition: cannot transition from %v to STARTING", df.State)
	}
	df.State = Starting
	df.StateTimestamp = time.Now().UnixMilli()
	df.StateCount++
	return nil
}

func (df *DataFlow) TransitionToStarted() error {
	if df.State == Started {
		return nil
	}
	if df.State != Uninitialized && df.State != Prepared && df.State != Starting && df.State != Suspended {
		return fmt.Errorf("invalid transition: cannot transition from %v to STARTED", df.State)
	}
	df.State = Started
	df.StateTimestamp = time.Now().UnixMilli()
	df.StateCount++
	return nil
}

func (df *DataFlow) TransitionToSuspended() error {
	if df.State == Suspended {
		return nil
	}
	if df.State != Started {
		return fmt.Errorf("invalid transition: cannot transition from %v to SUSPENDED", df.State)
	}
	df.State = Suspended
	df.StateTimestamp = time.Now().UnixMilli()
	df.StateCount++
	return nil
}

func (df *DataFlow) TransitionToCompleted() error {
	if df.State == Completed {
		return nil
	}
	if df.State != Started {
		return fmt.Errorf("invalid transition: cannot transition from %v to COMPLETED", df.State)
	}
	df.State = Completed
	df.StateTimestamp = time.Now().UnixMilli()
	df.StateCount++
	return nil
}

func (df *DataFlow) TransitionToTerminated() error {
	if df.State == Terminated {
		return nil // todo: does returning an error make sense here?
	}
	// Any state can transition to terminated
	df.State = Terminated
	df.StateTimestamp = time.Now().UnixMilli()
	df.StateCount++
	return nil
}

type DataFlowBuilder struct {
	dataFlow DataFlow
}

func NewDataFlowBuilder() *DataFlowBuilder {
	return &DataFlowBuilder{}
}

func (b *DataFlowBuilder) ID(id string) *DataFlowBuilder {
	b.dataFlow.ID = id
	return b
}

func (b *DataFlowBuilder) Consumer(val bool) *DataFlowBuilder {
	b.dataFlow.Consumer = val
	return b
}

func (b *DataFlowBuilder) UpdatedAt(updatedAt int64) *DataFlowBuilder {
	b.dataFlow.UpdatedAt = updatedAt
	return b
}

func (b *DataFlowBuilder) CreatedAt(createdAt int64) *DataFlowBuilder {
	b.dataFlow.CreatedAt = createdAt
	return b
}

func (b *DataFlowBuilder) ParticipantID(id string) *DataFlowBuilder {
	b.dataFlow.ParticipantID = id
	return b
}

func (b *DataFlowBuilder) DataspaceContext(context string) *DataFlowBuilder {
	b.dataFlow.DataspaceContext = context
	return b
}

func (b *DataFlowBuilder) CounterpartyID(id string) *DataFlowBuilder {
	b.dataFlow.CounterPartyID = id
	return b
}

func (b *DataFlowBuilder) State(state DataFlowState) *DataFlowBuilder {
	b.dataFlow.State = state
	return b
}

func (b *DataFlowBuilder) StateCount(count uint) *DataFlowBuilder {
	b.dataFlow.StateCount = count
	return b
}

func (b *DataFlowBuilder) StateTimestamp(timestamp int64) *DataFlowBuilder {
	b.dataFlow.StateTimestamp = timestamp
	return b
}

func (b *DataFlowBuilder) SourceDataAddress(address DataAddress) *DataFlowBuilder {
	b.dataFlow.SourceDataAddress = address
	return b
}

func (b *DataFlowBuilder) DestinationDataAddress(address DataAddress) *DataFlowBuilder {
	b.dataFlow.DestinationDataAddress = address
	return b
}

func (b *DataFlowBuilder) CallbackAddress(callback CallbackURL) *DataFlowBuilder {
	b.dataFlow.CallbackAddress = callback
	return b
}

func (b *DataFlowBuilder) TransferType(transferType TransferType) *DataFlowBuilder {
	b.dataFlow.TransferType = transferType
	return b
}

func (b *DataFlowBuilder) ErrorDetail(error string) *DataFlowBuilder {
	b.dataFlow.ErrorDetail = error
	return b
}

func (b *DataFlowBuilder) RuntimeID(id string) *DataFlowBuilder {
	b.dataFlow.RuntimeID = id
	return b
}

func (b *DataFlowBuilder) DatasetID(id string) *DataFlowBuilder {
	b.dataFlow.DatasetID = id
	return b
}

func (b *DataFlowBuilder) AgreementID(id string) *DataFlowBuilder {
	b.dataFlow.AgreementID = id
	return b
}

func (b *DataFlowBuilder) Build() (*DataFlow, error) {
	var validationErrs []string

	if b.dataFlow.CreatedAt == 0 {
		b.dataFlow.CreatedAt = time.Now().UnixMilli()
	}

	if b.dataFlow.UpdatedAt == 0 {
		b.dataFlow.UpdatedAt = b.dataFlow.CreatedAt
	}

	if b.dataFlow.StateTimestamp == 0 {
		b.dataFlow.StateTimestamp = b.dataFlow.CreatedAt
	}

	if b.dataFlow.ID == "" {
		validationErrs = append(validationErrs, "ID is required")
	}

	if b.dataFlow.ParticipantID == "" {
		validationErrs = append(validationErrs, "ParticipantID is required")
	}

	if b.dataFlow.DataspaceContext == "" {
		validationErrs = append(validationErrs, "DataspaceContext is required")
	}

	if b.dataFlow.CounterPartyID == "" {
		validationErrs = append(validationErrs, "CounterPartyID is required")
	}

	if b.dataFlow.CallbackAddress.URL().String() == "" {
		validationErrs = append(validationErrs, "CallbackAddress is required")
	}

	if b.dataFlow.TransferType.DestinationType == "" {
		validationErrs = append(validationErrs, "TransferType destination type is required")
	}

	if b.dataFlow.TransferType.FlowType == "" {
		validationErrs = append(validationErrs, "TransferType flow type is required")
	}

	if len(validationErrs) > 0 {
		return nil, fmt.Errorf("validation failed: %v", validationErrs)
	}

	return &b.dataFlow, nil
}

type CallbackURL url.URL

func (u CallbackURL) MarshalJSON() ([]byte, error) {
	//	Convert to standard url.URL and get the string representation
	stdURL := url.URL(u)
	return json.Marshal(stdURL.String())
}

func (u *CallbackURL) UnmarshalJSON(data []byte) error {
	var urlStr string
	if err := json.Unmarshal(data, &urlStr); err != nil {
		return err
	}

	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return err
	}

	*u = CallbackURL(*parsedURL)
	return nil
}

func (u *CallbackURL) URL() *url.URL {
	if u == nil {
		return nil
	}

	urlCopy := url.URL(*u)
	return &urlCopy
}
