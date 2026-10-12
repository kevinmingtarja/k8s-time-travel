// Package client talks to the ktt daemon over its unix socket, starting the
// daemon if it is not running.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/kevinmingtarja/k8s-time-travel/internal/api"
	"github.com/kevinmingtarja/k8s-time-travel/internal/server"
	"github.com/kevinmingtarja/k8s-time-travel/internal/store"
)

type Client struct {
	home string
	http *http.Client
}

func New(home string) *Client {
	sock := server.SocketPath(home)
	return &Client{
		home: home,
		http: &http.Client{Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", sock)
			},
		}},
	}
}

// Ensure pings the daemon and starts it if nothing is listening.
func (c *Client) Ensure() error {
	if _, err := c.Ping(); err == nil {
		return nil
	}
	if err := os.MkdirAll(c.home, 0o700); err != nil {
		return err
	}
	logPath := filepath.Join(c.home, "daemon.log")
	logf, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer logf.Close()
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.Command(exe, "daemon", "--home", c.home)
	cmd.Stdout, cmd.Stderr = logf, logf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start daemon: %w", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case err := <-exited:
			return fmt.Errorf("daemon exited during startup (%v); see %s", err, logPath)
		case <-deadline:
			return fmt.Errorf("daemon did not answer within 10s; see %s", logPath)
		case <-time.After(50 * time.Millisecond):
		}
		if p, err := c.Ping(); err == nil {
			fmt.Fprintf(os.Stderr, "ktt: started daemon (pid %d, log %s)\n", p.PID, logPath)
			return nil
		}
	}
}

func (c *Client) do(method, path string, q url.Values, body, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	u := "http://ktt" + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequest(method, u, rd)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		var e server.ErrorResponse
		if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
			return fmt.Errorf("daemon returned %s", resp.Status)
		}
		return fmt.Errorf("%s", e.Error)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *Client) Ping() (api.PingResponse, error) {
	var p api.PingResponse
	err := c.do("GET", "/v1/ping", nil, nil, &p)
	return p, err
}

func (c *Client) Shutdown() error { return c.do("POST", "/v1/shutdown", nil, nil, nil) }

func (c *Client) Record(req server.RecordRequest) ([]api.Recording, error) {
	var out []api.Recording
	err := c.do("POST", "/v1/recordings", nil, req, &out)
	return out, err
}

func (c *Client) Recordings() ([]api.Recording, error) {
	var out []api.Recording
	err := c.do("GET", "/v1/recordings", nil, nil, &out)
	return out, err
}

func (c *Client) Stop(id string) ([]api.Recording, error) {
	var out []api.Recording
	err := c.do("DELETE", "/v1/recordings/"+url.PathEscape(id), nil, nil, &out)
	return out, err
}

func filterQuery(f store.Filter) url.Values {
	q := url.Values{}
	for k, v := range map[string]string{"context": f.Context, "gvr": f.GVR, "namespace": f.Namespace, "name": f.Name} {
		if v != "" {
			q.Set(k, v)
		}
	}
	return q
}

// History returns revisions with since <= TS <= until and TS > after.
func (c *Client) History(f store.Filter, since, until, after int64) (api.RevisionsResponse, error) {
	q := filterQuery(f)
	q.Set("since", strconv.FormatInt(since, 10))
	q.Set("until", strconv.FormatInt(until, 10))
	q.Set("after", strconv.FormatInt(after, 10))
	var out api.RevisionsResponse
	err := c.do("GET", "/v1/revisions", q, nil, &out)
	return out, err
}

// State returns each matching object's newest revision with TS <= at.
func (c *Client) State(f store.Filter, at int64) (api.RevisionsResponse, error) {
	q := filterQuery(f)
	q.Set("at", strconv.FormatInt(at, 10))
	var out api.RevisionsResponse
	err := c.do("GET", "/v1/state", q, nil, &out)
	return out, err
}

func (c *Client) Gaps(context, gvr string, since, until int64) ([]api.Gap, error) {
	q := url.Values{}
	if context != "" {
		q.Set("context", context)
	}
	if gvr != "" {
		q.Set("gvr", gvr)
	}
	q.Set("since", strconv.FormatInt(since, 10))
	q.Set("until", strconv.FormatInt(until, 10))
	var out []api.Gap
	err := c.do("GET", "/v1/gaps", q, nil, &out)
	return out, err
}
