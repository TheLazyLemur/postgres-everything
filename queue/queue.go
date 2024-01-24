package queue

import (
	"bytes"
	"context"
	"database/sql"
	"html/template"

	_ "github.com/lib/pq"
)

type QueueApi interface {
	Enqueue(d []byte) error
	Dequeue() ([]byte, error)
	EnqueueTx(ctx context.Context, tx *sql.Tx, d []byte) error
	DequeueTx(ctx context.Context, tx *sql.Tx) ([]byte, error)
	GetTx() (*sql.Tx, error)
	Close()
}

type queue struct {
	db        *sql.DB
	tableName string
	queueName string
}

func NewQueue(conStr string, tableName string, queueName string) (QueueApi, error) {
	query := `
	CREATE TABLE IF NOT EXISTS {{.TableName}} (
		id UUID PRIMARY KEY,
		inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
		message_type TEXT NOT NULL,
		message_payload BYTEA
	);

	CREATE INDEX IF NOT EXISTS inserted_at_idx ON {{.TableName}} (inserted_at asc);
	CREATE INDEX IF NOT EXISTS message_type_idx ON {{.TableName}} (message_type);
	`

	sqlTmpl, err := template.New("sql").Parse(query)
	if err != nil {
		return nil, err
	}

	stuff := struct {
		TableName string
	}{
		TableName: tableName,
	}

	var outputSqlBuffer bytes.Buffer
	err = sqlTmpl.Execute(&outputSqlBuffer, stuff)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("postgres", conStr)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(outputSqlBuffer.String())
	if err != nil {
		return nil, err
	}

	return &queue{
		db:        db,
		tableName: tableName,
		queueName: queueName,
	}, nil
}

func (q *queue) Enqueue(d []byte) (err error) {
	query := `
	INSERT INTO {{.TableName}} (id, message_type, message_payload)
	VALUES
	(GEN_RANDOM_UUID(), $1, $2)
	`

	sqlTmpl, err := template.New("sql").Parse(query)
	if err != nil {
		return err
	}

	stuff := struct {
		TableName string
	}{
		TableName: q.tableName,
	}

	var outputSqlBuffer bytes.Buffer
	err = sqlTmpl.Execute(&outputSqlBuffer, stuff)
	if err != nil {
		return err
	}

	tx, err := q.db.Begin()
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			_ = tx.Commit()
		}
	}()

	_, err = tx.Exec(outputSqlBuffer.String(), q.queueName, d)
	if err != nil {
		return err
	}

	return nil
}

func (q *queue) Dequeue() (data []byte, err error) {
	query := `
	DELETE FROM {{.TableName}} qt
	WHERE qt.id =
		(SELECT qt_inner.id
		FROM {{.TableName}} qt_inner
		WHERE qt_inner.message_type = $1
		ORDER BY qt_inner.inserted_at ASC
			FOR UPDATE SKIP LOCKED
		LIMIT 1)
	RETURNING qt.message_payload
	`

	sqlTmpl, err := template.New("sql").Parse(query)
	if err != nil {
		return nil, err
	}

	stuff := struct {
		TableName string
	}{
		TableName: q.tableName,
	}

	var sqlOutputBuffer bytes.Buffer
	err = sqlTmpl.Execute(&sqlOutputBuffer, stuff)
	if err != nil {
		return nil, err
	}

	res := make([]byte, 0)

	tx, err := q.db.Begin()
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			_ = tx.Commit()
		}
	}()

	row := tx.QueryRow(sqlOutputBuffer.String(), q.queueName)

	err = row.Scan(&res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (q *queue) EnqueueTx(ctx context.Context, tx *sql.Tx, d []byte) (err error) {
	query := `
	INSERT INTO {{.TableName}} (id, message_type, message_payload)
	VALUES
	(GEN_RANDOM_UUID(), $1, $2)
	`

	sqlTmpl, err := template.New("sql").Parse(query)
	if err != nil {
		return err
	}

	stuff := struct {
		TableName string
	}{
		TableName: q.tableName,
	}

	var sqlOutputBuffer bytes.Buffer
	err = sqlTmpl.Execute(&sqlOutputBuffer, stuff)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, sqlOutputBuffer.String(), q.queueName, d)
	if err != nil {
		return err
	}

	return nil
}

func (q *queue) DequeueTx(ctx context.Context, tx *sql.Tx) (data []byte, err error) {
	query := `
	DELETE FROM {{.TableName}} qt
	WHERE qt.id =
		(SELECT qt_inner.id
		FROM {{.TableName}} qt_inner
		WHERE qt_inner.message_type = $1
		ORDER BY qt_inner.inserted_at ASC
			FOR UPDATE SKIP LOCKED
		LIMIT 1)
	RETURNING qt.message_payload
	`

	sqlTmpl, err := template.New("sql").Parse(query)
	if err != nil {
		return nil, err
	}

	stuff := struct {
		TableName string
	}{
		TableName: q.tableName,
	}

	var sqlOutputBuffer bytes.Buffer
	err = sqlTmpl.Execute(&sqlOutputBuffer, stuff)
	if err != nil {
		return nil, err
	}

	res := make([]byte, 0)

	row := tx.QueryRowContext(ctx, sqlOutputBuffer.String(), q.queueName)

	err = row.Scan(&res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (q *queue) GetTx() (*sql.Tx, error) {
	return q.db.Begin()
}

func (q *queue) Close() {
	q.db.Close()
}
