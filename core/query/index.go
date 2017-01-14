package query

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/lib/pq"

	"chain/database/pg"
	"chain/errors"
	"chain/protocol/bc"
)

const (
	// TxPinName is used to identify the pin associated
	// with the transaction block processor.
	TxPinName = "tx"
)

// Annotator describes a function capable of adding annotations
// to transactions, inputs and outputs.
type Annotator func(ctx context.Context, txs []*AnnotatedTx) error

// RegisterAnnotator adds an additional annotator capable of mutating
// the annotated transaction object.
func (ind *Indexer) RegisterAnnotator(annotator Annotator) {
	ind.annotators = append(ind.annotators, annotator)
}

func (ind *Indexer) ProcessBlocks(ctx context.Context) {
	if ind.pinStore == nil {
		return
	}
	ind.pinStore.ProcessBlocks(ctx, ind.c, TxPinName, ind.IndexTransactions)
}

// IndexTransactions is registered as a block callback on the Chain. It
// saves all annotated transactions to the database.
func (ind *Indexer) IndexTransactions(ctx context.Context, b *bc.Block) error {
	<-ind.pinStore.PinWaiter("asset", b.Height)

	err := ind.insertBlock(ctx, b)
	if err != nil {
		return err
	}

	txs, err := ind.insertAnnotatedTxs(ctx, b)
	if err != nil {
		return err
	}

	return ind.insertAnnotatedOutputs(ctx, b, txs)
}

func (ind *Indexer) insertBlock(ctx context.Context, b *bc.Block) error {
	const q = `
		INSERT INTO query_blocks (height, timestamp) VALUES($1, $2)
		ON CONFLICT (height) DO NOTHING
	`
	_, err := ind.db.Exec(ctx, q, b.Height, b.TimestampMS)
	return errors.Wrap(err, "inserting block timestamp")
}

func (ind *Indexer) insertAnnotatedTxs(ctx context.Context, b *bc.Block) ([]*AnnotatedTx, error) {
	var (
		hashes           = pq.ByteaArray(make([][]byte, 0, len(b.Transactions)))
		positions        = pg.Uint32s(make([]uint32, 0, len(b.Transactions)))
		annotatedTxBlobs = pq.StringArray(make([]string, 0, len(b.Transactions)))
		annotatedTxs     = make([]*AnnotatedTx, 0, len(b.Transactions))
		locals           = pq.BoolArray(make([]bool, 0, len(b.Transactions)))
		referenceDatas   = pq.StringArray(make([]string, 0, len(b.Transactions)))
	)

	// Build the fully annotated transactions.
	for pos, tx := range b.Transactions {
		annotatedTxs = append(annotatedTxs, buildAnnotatedTransaction(tx, b, uint32(pos)))
	}
	for _, annotator := range ind.annotators {
		err := annotator(ctx, annotatedTxs)
		if err != nil {
			return nil, errors.Wrap(err, "adding external annotations")
		}
	}
	localAnnotator(ctx, annotatedTxs)

	// Collect the fields we need to commit to the DB.
	for pos, tx := range annotatedTxs {
		b, err := json.Marshal(tx)
		if err != nil {
			return nil, err
		}
		annotatedTxBlobs = append(annotatedTxBlobs, string(b))
		hashes = append(hashes, tx.ID)
		positions = append(positions, uint32(pos))
		locals = append(locals, bool(tx.IsLocal))
		referenceDatas = append(referenceDatas, string(*tx.ReferenceData))
	}

	// Save the annotated txs to the database.
	const insertQ = `
		INSERT INTO annotated_txs(block_height, block_id, timestamp,
			tx_pos, tx_hash, data, local, reference_data)
		SELECT $1, $2, $3, unnest($4::integer[]), unnest($5::bytea[]),
			unnest($6::jsonb[]), unnest($7::boolean[]), unnest($8::jsonb[])
		ON CONFLICT (block_height, tx_pos) DO NOTHING;
	`
	_, err := ind.db.Exec(ctx, insertQ, b.Height, b.Hash(), b.Time(), positions,
		hashes, annotatedTxBlobs, locals, referenceDatas)
	if err != nil {
		return nil, errors.Wrap(err, "inserting annotated_txs to db")
	}
	return annotatedTxs, nil
}

func (ind *Indexer) insertAnnotatedOutputs(ctx context.Context, b *bc.Block, annotatedTxs []*AnnotatedTx) error {
	var (
		outputTxPositions      pg.Uint32s
		outputIndexes          pg.Uint32s
		outputTxHashes         pq.ByteaArray
		outputTypes            pq.StringArray
		outputPurposes         []sql.NullString
		outputAssetIDs         pq.ByteaArray
		outputAssetAliases     []sql.NullString
		outputAssetDefinitions pq.StringArray
		outputAssetTags        pq.StringArray
		outputAssetLocals      pq.BoolArray
		outputAmounts          pq.Int64Array
		outputAccountIDs       []sql.NullString
		outputAccountAliases   []sql.NullString
		outputAccountTags      []sql.NullString
		outputControlPrograms  pq.ByteaArray
		outputReferenceDatas   pq.StringArray
		outputLocals           pq.BoolArray
		outputDatas            pq.StringArray
		prevoutHashes          pq.ByteaArray
		prevoutIndexes         pg.Uint32s
	)

	for pos, tx := range b.Transactions {
		for _, in := range tx.Inputs {
			if !in.IsIssuance() {
				outpoint := in.Outpoint()
				prevoutHashes = append(prevoutHashes, outpoint.Hash[:])
				prevoutIndexes = append(prevoutIndexes, outpoint.Index)
			}
		}

		for outIndex, out := range annotatedTxs[pos].Outputs {
			if out.Type == "retire" {
				continue
			}

			outCopy := *out
			outCopy.TransactionID = tx.Hash[:]
			serializedData, err := json.Marshal(outCopy)
			if err != nil {
				return errors.Wrap(err, "serializing annotated output")
			}

			outputTxPositions = append(outputTxPositions, uint32(pos))
			outputIndexes = append(outputIndexes, uint32(outIndex))
			outputTxHashes = append(outputTxHashes, tx.Hash[:])
			outputTypes = append(outputTypes, out.Type)
			outputPurposes = append(outputPurposes, sql.NullString{String: out.Purpose, Valid: out.Purpose != ""})
			outputAssetIDs = append(outputAssetIDs, out.AssetID)
			outputAssetAliases = append(outputAssetAliases, sql.NullString{String: out.AssetAlias, Valid: out.AssetAlias != ""})
			outputAssetDefinitions = append(outputAssetDefinitions, string(*out.AssetDefinition))
			outputAssetTags = append(outputAssetTags, string(*out.AssetTags))
			outputAssetLocals = append(outputAssetLocals, bool(out.AssetIsLocal))
			outputAmounts = append(outputAmounts, int64(out.Amount))
			outputAccountIDs = append(outputAccountIDs, sql.NullString{String: out.AccountID, Valid: out.AccountID != ""})
			outputAccountAliases = append(outputAccountAliases, sql.NullString{String: out.AccountAlias, Valid: out.AccountAlias != ""})
			if out.AccountTags != nil {
				outputAccountTags = append(outputAccountTags, sql.NullString{String: string(*out.AccountTags), Valid: true})
			} else {
				outputAccountTags = append(outputAccountTags, sql.NullString{})
			}
			outputControlPrograms = append(outputControlPrograms, out.ControlProgram)
			outputReferenceDatas = append(outputReferenceDatas, string(*out.ReferenceData))
			outputLocals = append(outputLocals, bool(out.IsLocal))
			outputDatas = append(outputDatas, string(serializedData))
		}
	}

	// Insert all of the block's outputs at once.
	const insertQ = `
		INSERT INTO annotated_outputs (block_height, tx_pos, output_index, tx_hash,
			data, timespan, type, purpose, asset_id, asset_alias, asset_definition,
			asset_tags, asset_local, amount, account_id, account_alias, account_tags,
			control_program, reference_data, local)
		SELECT $1, unnest($2::integer[]), unnest($3::integer[]), unnest($4::bytea[]),
		unnest($5::jsonb[]), int8range($6, NULL), unnest($7::text[]), unnest($8::bytea[]),
		unnest($9::bytea[]), unnest($10::text[]), unnest($11::jsonb[]), unnest($12::jsonb[]),
		unnest($13::boolean[]), unnest($14::bigint[]), unnest($15::text[]), unnest($16::text[]),
		unnest($17::jsonb[]), unnest($18::bytea[]), unnest($19::jsonb[]), unnest($20::boolean[])
		ON CONFLICT (block_height, tx_pos, output_index) DO NOTHING;
	`
	_, err := ind.db.Exec(ctx, insertQ, b.Height, outputTxPositions,
		outputIndexes, outputTxHashes, outputDatas, b.TimestampMS, outputTypes,
		pq.Array(outputPurposes), outputAssetIDs, pq.Array(outputAssetAliases),
		outputAssetDefinitions, outputAssetTags, outputAssetLocals,
		outputAmounts, pq.Array(outputAccountIDs), pq.Array(outputAccountAliases),
		pq.Array(outputAccountTags), outputControlPrograms, outputReferenceDatas,
		outputLocals)
	if err != nil {
		return errors.Wrap(err, "batch inserting annotated outputs")
	}

	const updateQ = `
		UPDATE annotated_outputs SET timespan = INT8RANGE(LOWER(timespan), $1)
		WHERE (tx_hash, output_index) IN (SELECT unnest($2::bytea[]), unnest($3::integer[]))
	`
	_, err = ind.db.Exec(ctx, updateQ, b.TimestampMS, prevoutHashes, prevoutIndexes)
	return errors.Wrap(err, "updating spent annotated outputs")
}
