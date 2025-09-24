-- DataFlow persistence (PostgreSQL)

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Main table for DataFlow
CREATE TABLE IF NOT EXISTS data_flows
(
    id                     TEXT PRIMARY KEY NOT NULL,           -- maps to DataFlow.ID
    version                BIGINT           NOT NULL DEFAULT 0, -- DataFlow.Version (optimistic locking)
    consumer               BOOLEAN          NOT NULL,           -- DataFlow.Consumer
    agreement_id           TEXT             NOT NULL,           -- DataFlow.AgreementID
    dataset_id             TEXT             NOT NULL,           -- DataFlow.DatasetID
    runtime_id             TEXT             NOT NULL,           -- DataFlow.RuntimeID
    participant_id         TEXT             NOT NULL,           -- DataFlow.ParticipantID
    dataspace_context      TEXT             NOT NULL,           -- DataFlow.DataspaceContext
    counterparty_id        TEXT             NOT NULL,           -- DataFlow.CounterPartyID

    callback_address       TEXT             NOT NULL,           -- DataFlow.CallbackAddress (URL as text)

    transfer_type_dest     VARCHAR          NOT NULL,           -- DataFlow.TransferType {destinationType}
    transfer_type_flowtype VARCHAR          NOT NULL,           -- DataFlow.TransferType {flowType}
    source_data_address    JSONB            NOT NULL,           -- DataFlow.SourceDataAddress {properties: {...}}
    dest_data_address      JSONB            NOT NULL,           -- DataFlow.DestinationDataAddress {properties: {...}}

    state                  INTEGER          NOT NULL DEFAULT 0, -- DataFlow.State (enum int)
    state_count            INTEGER          NOT NULL DEFAULT 0, -- DataFlow.StateCount
    state_timestamp_ms     BIGINT           NOT NULL,           -- DataFlow.StateTimestamp (epoch millis)

    error_detail           VARCHAR,                             -- DataFlow.ErrorDetail

    created_at_ms          BIGINT           NOT NULL,           -- DataFlow.CreatedAt (epoch millis)
    updated_at_ms          BIGINT           NOT NULL            -- DataFlow.UpdatedAt (epoch millis)
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_data_flows_state ON data_flows (state);
CREATE INDEX IF NOT EXISTS idx_data_flows_updated_at ON data_flows (updated_at_ms);
CREATE INDEX IF NOT EXISTS idx_data_flows_agreement ON data_flows (agreement_id);
CREATE INDEX IF NOT EXISTS idx_data_flows_dataset ON data_flows (dataset_id);
CREATE INDEX IF NOT EXISTS idx_data_flows_participant ON data_flows (participant_id);

-- Optional JSONB GIN indexes (uncomment if you need property-level queries)
-- CREATE INDEX IF NOT EXISTS idx_data_flows_transfer_type_gin ON data_flows USING GIN (transfer_type);
-- CREATE INDEX IF NOT EXISTS idx_data_flows_src_addr_gin ON data_flows USING GIN (source_data_address);
-- CREATE INDEX IF NOT EXISTS idx_data_flows_dst_addr_gin ON data_flows USING GIN (dest_data_address);