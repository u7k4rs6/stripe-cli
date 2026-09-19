package websocket

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	ws "github.com/gorilla/websocket"

	"github.com/stretchr/testify/require"
)

func TestClientWebhookEventHandler(t *testing.T) {
	upgrader := ws.Upgrader{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NotEmpty(t, r.UserAgent())
		require.NotEmpty(t, r.Header.Get("X-Stripe-Client-User-Agent"))
		require.Equal(t, "websocket-random-id", r.Header.Get("Websocket-Id"))
		c, err := upgrader.Upgrade(w, r, nil)
		require.NoError(t, err)

		require.Equal(t, "websocket_feature=webhook-payloads", r.URL.RawQuery)

		defer c.Close()

		evt := WebhookEvent{
			EventPayload: "{}",
			HTTPHeaders: map[string]string{
				"User-Agent":       "TestAgent/v1",
				"Stripe-Signature": "t=123,v1=hunter2",
			},
			Type: "webhook_event",
		}

		msg, err := json.Marshal(evt)
		require.NoError(t, err)

		err = c.WriteMessage(ws.TextMessage, msg)
		require.NoError(t, err)
	}))

	defer ts.Close()

	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	var rcvMsg WebhookEvent

	rcvMsgChan := make(chan WebhookEvent)

	client := NewClient(
		url,
		"websocket-random-id",
		"webhook-payloads",
		&Config{
			EventHandler: EventHandlerFunc(func(msg IncomingMessage) {
				rcvMsgChan <- *msg.WebhookEvent
			}),
		},
	)

	go client.Run(context.Background())

	defer client.Stop()

	select {
	case rcvMsg = <-rcvMsgChan:
	case <-time.After(500 * time.Millisecond):
		require.FailNow(t, "Timed out waiting for response from test server")
	}

	require.Equal(t, "TestAgent/v1", rcvMsg.HTTPHeaders["User-Agent"])
	require.Equal(t, "t=123,v1=hunter2", rcvMsg.HTTPHeaders["Stripe-Signature"])
	require.Equal(t, "{}", rcvMsg.EventPayload)
}

func TestClientWebhookV2EventHandler(t *testing.T) {
	upgrader := ws.Upgrader{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NotEmpty(t, r.UserAgent())
		require.NotEmpty(t, r.Header.Get("X-Stripe-Client-User-Agent"))
		require.Equal(t, "websocket-random-id", r.Header.Get("Websocket-Id"))
		c, err := upgrader.Upgrade(w, r, nil)
		require.NoError(t, err)

		require.Equal(t, "websocket_feature=webhook-payloads", r.URL.RawQuery)

		defer c.Close()

		evt := StripeV2Event{
			Payload: "{}",
			HTTPHeaders: map[string]string{
				"User-Agent":       "TestAgent/v1",
				"Stripe-Signature": "t=123,v1=hunter2",
			},
			Type: "v2_event",
		}

		msg, err := json.Marshal(evt)
		require.NoError(t, err)

		err = c.WriteMessage(ws.TextMessage, msg)
		require.NoError(t, err)
	}))

	defer ts.Close()

	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	var rcvMsg StripeV2Event

	rcvMsgChan := make(chan StripeV2Event)

	client := NewClient(
		url,
		"websocket-random-id",
		"webhook-payloads",
		&Config{
			EventHandler: EventHandlerFunc(func(msg IncomingMessage) {
				rcvMsgChan <- *msg.StripeV2Event
			}),
		},
	)

	go client.Run(context.Background())

	defer client.Stop()

	select {
	case rcvMsg = <-rcvMsgChan:
	case <-time.After(500 * time.Millisecond):
		require.FailNow(t, "Timed out waiting for response from test server")
	}

	require.Equal(t, "TestAgent/v1", rcvMsg.HTTPHeaders["User-Agent"])
	require.Equal(t, "t=123,v1=hunter2", rcvMsg.HTTPHeaders["Stripe-Signature"])
	require.Equal(t, "{}", rcvMsg.Payload)
}

func TestClientRequestLogEventHandler(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	upgrader := ws.Upgrader{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NotEmpty(t, r.UserAgent())
		require.NotEmpty(t, r.Header.Get("X-Stripe-Client-User-Agent"))
		require.Equal(t, "websocket-random-id", r.Header.Get("Websocket-Id"))
		c, err := upgrader.Upgrade(w, r, nil)
		require.NoError(t, err)

		require.Equal(t, "websocket_feature=request-log-payloads", r.URL.RawQuery)

		defer c.Close()

		evt := RequestLogEvent{
			EventPayload: "{}",
			RequestLogID: "resp_123",
			Type:         "request_log_event",
		}

		msg, err := json.Marshal(evt)
		require.NoError(t, err)

		err = c.WriteMessage(ws.TextMessage, msg)
		require.NoError(t, err)
	}))

	defer ts.Close()

	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	var rcvMsg *RequestLogEvent

	client := NewClient(
		url,
		"websocket-random-id",
		"request-log-payloads",
		&Config{
			EventHandler: EventHandlerFunc(func(msg IncomingMessage) {
				rcvMsg = msg.RequestLogEvent
				wg.Done()
			}),
		},
	)

	go client.Run(context.Background())

	defer client.Stop()

	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		require.FailNow(t, "Timed out waiting for response from test server")
	}

	require.Equal(t, "resp_123", rcvMsg.RequestLogID)
	require.Equal(t, "request_log_event", rcvMsg.Type)
	require.Equal(t, "{}", rcvMsg.EventPayload)
}

func TestClientExpiredError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		_, err := w.Write([]byte("{\"error\": {\"message\": \"Unknown WebSocket ID.\"}}"))
		require.NoError(t, err)
	}))

	defer ts.Close()

	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	client := NewClient(
		url,
		"websocket-random-id",
		"webhook-payloads",
		&Config{
			ConnectAttemptWait: 1,
		},
	)

	go client.Run(context.Background())

	select {
	case <-client.NotifyExpired:
	case <-time.After(500 * time.Millisecond):
		require.FailNow(t, "Timed out waiting for response from test server")
	}
}

// echoServer is a websocket test server that holds the connection open and
// drains whatever the client writes, so tests can exercise the send path.
func echoServer(t *testing.T) *httptest.Server {
	t.Helper()

	upgrader := ws.Upgrader{}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		defer c.Close()

		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}))
}

// Event handlers call SendMessage from the read pump's goroutines, so it runs
// concurrently with the shutdown in Run that closes c.send. It must drop the
// message rather than panic with "send on closed channel".
func TestClientSendMessageRacesWithShutdown(t *testing.T) {
	ts := echoServer(t)
	defer ts.Close()

	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	client := NewClient(
		url,
		"websocket-random-id",
		"webhook-payloads",
		&Config{
			NoWSS:            true,
			CloseDelayPeriod: 1 * time.Millisecond,
		},
	)

	ctx, cancel := context.WithCancel(context.Background())

	go client.Run(ctx)

	<-client.Connected()

	var wg sync.WaitGroup

	stop := make(chan struct{})

	for i := 0; i < 8; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case <-stop:
					return
				default:
				}

				client.SendMessage(NewEventAck("evt_123", "", ""))
			}
		}()
	}

	time.Sleep(20 * time.Millisecond)
	cancel()
	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// SendMessage must also stay safe once shutdown has finished, not only while
// it is racing the close.
func TestClientSendMessageAfterShutdown(t *testing.T) {
	ts := echoServer(t)
	defer ts.Close()

	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	client := NewClient(
		url,
		"websocket-random-id",
		"webhook-payloads",
		&Config{
			NoWSS:            true,
			CloseDelayPeriod: 1 * time.Millisecond,
		},
	)

	ctx, cancel := context.WithCancel(context.Background())

	go client.Run(ctx)

	<-client.Connected()

	cancel()
	time.Sleep(100 * time.Millisecond)

	client.SendMessage(NewEventAck("evt_123", "", ""))
}

// The expired-session notification is a blocking send, so Run must not park on
// it forever once the caller has stopped receiving.
func TestClientNotifyExpiredDoesNotBlockWithoutConsumer(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		_, err := w.Write([]byte("{\"error\": {\"message\": \"Unknown WebSocket ID.\"}}"))
		require.NoError(t, err)
	}))

	defer ts.Close()

	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	client := NewClient(
		url,
		"websocket-random-id",
		"webhook-payloads",
		&Config{
			ConnectAttemptWait: 1 * time.Millisecond,
		},
	)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})

	go func() {
		defer close(done)

		client.Run(ctx)
	}()

	// Nothing ever receives on NotifyExpired. Canceling stands in for the
	// caller returning on its own context, which is what leaves this send
	// without a consumer.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "Run leaked a goroutine parked on NotifyExpired")
	}
}
