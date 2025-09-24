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

	payload, err := serialize(createStartMessage())
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
	sm := createStartMessage()
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

func serialize(obj any) ([]byte, error) {
	marshal, err := json.Marshal(obj)
	return marshal, err
}

func createStartMessage() dsdk.DataFlowStartMessage {
	sm := dsdk.DataFlowStartMessage{
		DataFlowBaseMessage: dsdk.DataFlowBaseMessage{
			MessageID:        uuid.New().String(),
			ParticipantID:    uuid.New().String(),
			CounterPartyID:   uuid.New().String(),
			DataspaceContext: uuid.New().String(),
			ProcessID:        uuid.New().String(),
			AgreementID:      uuid.New().String(),
			DatasetID:        uuid.New().String(),
			CallbackAddress:  dsdk.CallbackURL{Scheme: "http", Host: "test.com", Path: "/callback"},
			TransferType: dsdk.TransferType{
				DestinationType: "com.test.http",
				FlowType:        "pull",
			},
			DestinationDataAddress: dsdk.DataAddress{},
		},
		SourceDataAddress: &dsdk.DataAddress{},
	}
	return sm
}

func createSdk(db *sql.DB) (*dsdk.DataPlaneSDK, error) {
	sdk, err := dsdk.NewDataPlaneSDKBuilder().
		Store(postgres.NewStore(db)).
		TransactionContext(postgres.NewDBTransactionContext(db)).
		Build()
	return sdk, err
}
