//go:build postgres

package tests

import (
	"bytes"
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/metaform/dataplane-sdk-go/pkg/dsdk"
	"github.com/metaform/dataplane-sdk-go/pkg/postgres"
	"github.com/stretchr/testify/assert"
)

// TestMain ensures containers are cleaned up and allows global setup if desired.
var ctx = context.Background()

// database SQL database connection, used for test setup and assertionss
var database *sql.DB

// newServerWithSdk instantiates a new HTTP server using the DataPlane SDK and registers its callbacks with endpoints
func newServerWithSdk(t *testing.T, sdk *dsdk.DataPlaneSDK) http.Handler {
	t.Helper()
	sdkApi := dsdk.NewDataPlaneApi(sdk)
	mux := http.NewServeMux()

	mux.HandleFunc("/start", sdkApi.Start)
	mux.HandleFunc("/prepare", sdkApi.Prepare)
	mux.HandleFunc("/terminate/", sdkApi.Terminate)
	mux.HandleFunc("/suspend/", sdkApi.Suspend)
	mux.HandleFunc("/status", sdkApi.Status)
	return mux
}

var handler http.Handler

func TestMain(m *testing.M) {
	db, container := postgres.SetupDatabase(&testing.T{}, ctx)
	database = db
	t := &testing.T{}

	sdk, err := createSdk(db)
	assert.NoError(t, err)
	handler = newServerWithSdk(t, sdk)
	code := m.Run()
	_ = db.Close()
	_ = container.Terminate(ctx)
	os.Exit(code)
}

// E2E tests
func Test_Start_NotExists(t *testing.T) {

	payload, err := serialize(newStartMessage())
	assert.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, "/start", bytes.NewBuffer(payload))
	assert.NoError(t, err)

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var responseMessage dsdk.DataFlowResponseMessage
	err = json.NewDecoder(rr.Body).Decode(&responseMessage)
	assert.NoError(t, err)
	assert.Equal(t, responseMessage.State, dsdk.Started)
}

func Test_Start_InvalidPayload(t *testing.T) {
	sm := newStartMessage()
	sm.CounterPartyID = ""
	payload, err := serialize(sm)
	assert.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, "/start", bytes.NewBuffer(payload))
	assert.NoError(t, err)

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.NotNil(t, rr.Body.String())
}

func Test_Prepare(t *testing.T) {
	payload, err := serialize(newPrepareMessage())
	assert.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, "/prepare", bytes.NewBuffer(payload))
	assert.NoError(t, err)

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var responseMessage dsdk.DataFlowResponseMessage
	err = json.NewDecoder(rr.Body).Decode(&responseMessage)
	assert.NoError(t, err)
	assert.Equal(t, responseMessage.State, dsdk.Prepared)
}

func Test_Prepare_WrongState(t *testing.T) {

	s := postgres.NewStore(database)
	flow, err := newFlowBuilder().State(dsdk.Started).Build()
	assert.NoError(t, err)
	assert.NoError(t, s.Create(ctx, flow))

	message := newPrepareMessage()
	message.ProcessID = flow.ID
	payload, err := serialize(message)
	assert.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, "/prepare", bytes.NewBuffer(payload))
	assert.NoError(t, err)

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusConflict, rr.Code)
}

func newFlowBuilder() *dsdk.DataFlowBuilder {
	bldr := &dsdk.DataFlowBuilder{}
	return bldr.ID("test-id").
		ParticipantID(uuid.New().String()).
		DataspaceContext(uuid.New().String()).
		CounterpartyID(uuid.New().String()).
		CallbackAddress(newCallback()).
		TransferType(newTransferType())
}

func serialize(obj any) ([]byte, error) {
	marshal, err := json.Marshal(obj)
	return marshal, err
}

func newStartMessage() dsdk.DataFlowStartMessage {
	return dsdk.DataFlowStartMessage{
		DataFlowBaseMessage: dsdk.DataFlowBaseMessage{
			MessageID:              uuid.New().String(),
			ParticipantID:          uuid.New().String(),
			CounterPartyID:         uuid.New().String(),
			DataspaceContext:       uuid.New().String(),
			ProcessID:              uuid.New().String(),
			AgreementID:            uuid.New().String(),
			DatasetID:              uuid.New().String(),
			CallbackAddress:        newCallback(),
			TransferType:           newTransferType(),
			DestinationDataAddress: dsdk.DataAddress{},
		},
		SourceDataAddress: &dsdk.DataAddress{},
	}
}

func newTransferType() dsdk.TransferType {
	return dsdk.TransferType{
		DestinationType: "com.test.http",
		FlowType:        "pull",
	}
}

func newPrepareMessage() dsdk.DataFlowPrepareMessage {
	return dsdk.DataFlowPrepareMessage{
		DataFlowBaseMessage: dsdk.DataFlowBaseMessage{
			MessageID:              uuid.New().String(),
			ParticipantID:          uuid.New().String(),
			CounterPartyID:         uuid.New().String(),
			DataspaceContext:       uuid.New().String(),
			ProcessID:              uuid.New().String(),
			AgreementID:            uuid.New().String(),
			DatasetID:              uuid.New().String(),
			CallbackAddress:        newCallback(),
			TransferType:           newTransferType(),
			DestinationDataAddress: dsdk.DataAddress{},
		},
	}
}

func newCallback() dsdk.CallbackURL {
	return dsdk.CallbackURL{Scheme: "http", Host: "test.com", Path: "/callback"}
}

func createSdk(db *sql.DB) (*dsdk.DataPlaneSDK, error) {
	sdk, err := dsdk.NewDataPlaneSDKBuilder().
		Store(postgres.NewStore(db)).
		TransactionContext(postgres.NewDBTransactionContext(db)).
		Build()
	return sdk, err
}
