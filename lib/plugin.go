package mongodb_stream_benthos

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"github.com/Jeffail/benthos/v3/public/service"
	"github.com/go-mysql-org/go-mysql/canal"
)

var mongoStreamConfigSpec = service.NewConfigSpec().
	Summary("Creates an input that generates mysql CDC stream").
	Field(service.NewStringField("addr")).
	Field(service.NewStringField("database")).
	Field(service.NewStringField("user")).
	Field(service.NewStringField("password")).
	Field(service.NewStringListField("tables")).
	Field(service.NewStringField("flavor")).
	Field(service.NewBoolField("stream_snapshot")).
	Field(service.NewBoolField("enable_ssl").Default(false))

type ProcessEventParams struct {
	initValue, incrementValue int
}

type StreamMessage struct {
	Table string         `json:"table"`
	Event string         `json:"event"`
	Data  map[string]any `json:"data"`
}

type mysqlStreamInput struct {
	addr      string
	user      string
	password  string
	database  string
	flavor    string
	enableSsl bool
	tables    []string
	canal     *canal.Canal
	canal.DummyEventHandler
	stream         chan StreamMessage
	streamSnapshot bool
}

func newMysqlStreamInput(conf *service.ParsedConfig) (service.Input, error) {
	var (
		addr           string
		user           string
		password       string
		database       string
		flavor         string
		enableSsl      bool
		tables         []string
		streamSnapshot bool
	)

	addr, err := conf.FieldString("addr")

	if err != nil {
		return nil, err
	}

	user, err = conf.FieldString("user")

	if err != nil {
		return nil, err
	}

	database, err = conf.FieldString("database")

	if err != nil {
		return nil, err
	}

	enableSsl, err = conf.FieldBool("enable_ssl")
	if err != nil {
		return nil, err
	}

	password, err = conf.FieldString("password")

	if err != nil {
		return nil, err
	}

	flavor, err = conf.FieldString("flavor")

	if err != nil {
		return nil, err
	}

	streamSnapshot, err = conf.FieldBool("stream_snapshot")
	if err != nil {
		return nil, err
	}

	return service.AutoRetryNacks(&mysqlStreamInput{
		addr:           addr,
		user:           user,
		password:       password,
		database:       database,
		flavor:         flavor,
		enableSsl:      enableSsl,
		tables:         tables,
		streamSnapshot: streamSnapshot,
		stream:         make(chan StreamMessage),
	}), nil
}

func init() {
	err := service.RegisterInput(
		"mysql_stream",
		mongoStreamConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return newMysqlStreamInput(conf)
		},
	)

	if err != nil {
		panic(err)
	}
}

func (m *mysqlStreamInput) Connect(ctx context.Context) error {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = m.addr
	cfg.User = m.user
	cfg.Password = m.password
	cfg.Dump.Tables = m.tables
	cfg.Dump.TableDB = m.database
	cfg.ServerID = 124
	cfg.Flavor = m.flavor
	if m.enableSsl {
		cfg.TLSConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	c, err := canal.NewCanal(cfg)

	if err != nil {
		return err
	}

	m.canal = c

	m.canal.SetEventHandler(m)
	go m.bingLogReader()
	return nil
}

func (m *mysqlStreamInput) Close(ctx context.Context) error {
	if m.canal != nil {
		m.canal.Close()
	}
	return nil
}

func (m *mysqlStreamInput) processEvent(e *canal.RowsEvent, params ProcessEventParams) error {
	for i := params.initValue; i < len(e.Rows); i += params.incrementValue {
		message := map[string]any{}
		for i, v := range e.Rows[i] {
			message[e.Table.Columns[i].Name] = v
		}

		m.stream <- StreamMessage{
			Table: e.Table.Name,
			Event: e.Action,
			Data:  message,
		}
	}
	return nil
}

func (m *mysqlStreamInput) OnRow(e *canal.RowsEvent) error {
	if m.database != e.Table.Schema {
		return nil
	}

	switch e.Action {
	case canal.InsertAction:
		return m.processEvent(e, ProcessEventParams{initValue: 0, incrementValue: 1})
	case canal.DeleteAction:
		return m.processEvent(e, ProcessEventParams{initValue: 0, incrementValue: 1})
	case canal.UpdateAction:
		return m.processEvent(e, ProcessEventParams{initValue: 1, incrementValue: 2})
	default:
		return errors.New("invalid rows action")
	}
}

func (m *mysqlStreamInput) bingLogReader() {
	if m.streamSnapshot {
		// Doesn't work at the moment
		if err := m.canal.Run(); err != nil {
			panic(err)
		}
	} else {
		coords, _ := m.canal.GetMasterPos()
		if err := m.canal.RunFrom(coords); err != nil {
			panic(err)
		}
	}
}

func (m *mysqlStreamInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	streamMessage := <-m.stream
	messageBodyEncoded, _ := json.Marshal(streamMessage.Data)
	createdMessage := service.NewMessage(messageBodyEncoded)
	createdMessage.MetaSet("table", streamMessage.Table)
	createdMessage.MetaSet("event", streamMessage.Event)

	return createdMessage, func(ctx context.Context, err error) error {
		return nil
	}, nil
}
