package beanstalk

import (
	"fmt"
	"io"
	"net"
	"net/textproto"
	"strings"
	"sync"
	"time"
)

// A Conn represents a connection to a beanstalkd server. It consists
// of a default Tube and TubeSet as well as the underlying network
// connection. The embedded types carry methods with them; see the
// documentation of those types for details.
type Conn struct {
	c       *textproto.Conn
	used    string
	watched map[string]bool
	mu      sync.Mutex // guards the connections
	addr    string
	network string
	Tube
	TubeSet
}

var (
	space             = []byte{' '}
	crnl              = []byte{'\r', '\n'}
	yamlHead          = []byte{'-', '-', '-', '\n'}
	nl                = []byte{'\n'}
	colonSpace        = []byte{':', ' '}
	minusSpace        = []byte{'-', ' '}
	connectRetries    = 120
	connectRetryDelay = 1 * time.Second
)

// NewConn returns a new Conn using conn for I/O.
func newConn(conn io.ReadWriteCloser, network, addr string) *Conn {
	c := new(Conn)
	c.c = textproto.NewConn(conn)
	c.Tube = Tube{c, "default"}
	c.TubeSet = *NewTubeSet(c, "default")
	c.used = "default"
	c.watched = map[string]bool{"default": true}
	c.network = network
	c.addr = addr
	return c
}

// Dial connects to the given address on the given network using net.Dial
// and then returns a new Conn for the connection.
func Dial(network, addr string) (*Conn, error) {
	c, err := net.Dial(network, addr)
	if err != nil {
		return nil, err
	}
	return newConn(c, network, addr), nil
}

// Close closes the underlying network connection.
func (c *Conn) Close() error {
	return c.c.Close()
}

// Try to re-establish a closed connection. This will attempt to re-establish
// for up to one minutes, so anything calling this should probably occur in a goroutine
// or be OK with blocking
func (c *Conn) Reconnect() (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < connectRetries; i++ {
		fmt.Println("Attempting to reconnect to beanstalk")
		if c.c, err = textproto.Dial(c.network, c.addr); err == nil {
			fmt.Println("Reconnecting to beanstalk")
			c.TubeSet = *NewTubeSet(c, "default")
			c.used = "default"
			c.watched = map[string]bool{"default": true}
			break
		} else {
			time.Sleep(connectRetryDelay)
		}
	}
	return
}

func (c *Conn) cmd(t *Tube, ts *TubeSet, body []byte, op string, args ...interface{}) (req, error) {
	r := req{c.c.Next(), op}
	c.c.StartRequest(r.id)
	err := c.adjustTubes(t, ts)
	if err != nil {
		return req{}, err
	}
	if body != nil {
		args = append(args, len(body))
	}
	c.printLine(string(op), args...)
	if body != nil {
		c.c.W.Write(body)
		c.c.W.Write(crnl)
	}
	err = c.c.W.Flush()
	if err != nil {
		return req{}, ConnError{c, op, err}
	}
	c.c.EndRequest(r.id)
	return r, nil
}

func (c *Conn) adjustTubes(t *Tube, ts *TubeSet) error {
	if t != nil && t.Name != c.used {
		if err := checkName(t.Name); err != nil {
			return err
		}
		c.printLine("use", t.Name)
		c.used = t.Name
	}
	if ts != nil {
		for s := range ts.Name {
			if !c.watched[s] {
				if err := checkName(s); err != nil {
					return err
				}
				c.printLine("watch", s)
			}
		}
		for s := range c.watched {
			if !ts.Name[s] {
				c.printLine("ignore", s)
			}
		}
		c.watched = make(map[string]bool)
		for s := range ts.Name {
			c.watched[s] = true
		}
	}
	return nil
}

// does not flush
func (c *Conn) printLine(cmd string, args ...interface{}) {
	io.WriteString(c.c.W, cmd)
	for _, a := range args {
		c.c.W.Write(space)
		fmt.Fprint(c.c.W, a)
	}
	c.c.W.Write(crnl)
}

func (c *Conn) readResp(r req, readBody bool, f string, a ...interface{}) (body []byte, err error) {
	c.c.StartResponse(r.id)
	defer c.c.EndResponse(r.id)
	line, err := c.c.ReadLine()
	for strings.HasPrefix(line, "WATCHING ") || strings.HasPrefix(line, "USING ") {
		line, err = c.c.ReadLine()
	}
	if err != nil {
		return nil, ConnError{c, r.op, err}
	}
	toScan := line
	if readBody {
		var size int
		toScan, size, err = parseSize(toScan)
		if err != nil {
			return nil, ConnError{c, r.op, err}
		}
		body = make([]byte, size+2) // include trailing CR NL
		_, err = io.ReadFull(c.c.R, body)
		if err != nil {
			return nil, ConnError{c, r.op, err}
		}
		body = body[:size] // exclude trailing CR NL
	}

	err = scan(toScan, f, a...)
	if err != nil {
		return nil, ConnError{c, r.op, err}
	}
	return body, nil
}

// Delete deletes the given job.
func (c *Conn) Delete(id uint64) error {
	r, err := c.cmd(nil, nil, nil, "delete", id)
	if err != nil {
		return err
	}
	_, err = c.readResp(r, false, "DELETED")
	return err
}

// Release tells the server to perform the following actions:
// set the priority of the given job to pri, remove it from the list of
// jobs reserved by c, wait delay seconds, then place the job in the
// ready queue, which makes it available for reservation by any client.
func (c *Conn) Release(id uint64, pri uint32, delay time.Duration) error {
	r, err := c.cmd(nil, nil, nil, "release", id, pri, dur(delay))
	if err != nil {
		return err
	}
	_, err = c.readResp(r, false, "RELEASED")
	return err
} // TODO

// Bury places the given job in a holding area in the job's tube and
// sets its priority to pri. The job will not be scheduled again until it
// has been kicked; see also the documentation of Kick.
func (c *Conn) Bury(id uint64, pri uint32) error {
	r, err := c.cmd(nil, nil, nil, "bury", id, pri)
	if err != nil {
		return err
	}
	_, err = c.readResp(r, false, "BURIED")
	return err
} // TODO

// Touch resets the reservation timer for the given job.
// It is an error if the job isn't currently reserved by c.
// See the documentation of Reserve for more details.
func (c *Conn) touch(id uint64) error {
	r, err := c.cmd(nil, nil, nil, "touch", id)
	if err != nil {
		return err
	}
	_, err = c.readResp(r, false, "TOUCHED")
	return err
}

func (c *Conn) Touch(id uint64) error {
	err := c.touch(id)

	if err != nil && err.(ConnError).IsEOF() {
		if retryErr := c.Reconnect(); retryErr == nil {
			return c.Touch(id)
		}
	}
	return err
}

// Peek gets a copy of the specified job from the server.
func (c *Conn) peek(id uint64) (body []byte, err error) {
	r, err := c.cmd(nil, nil, nil, "peek", id)
	if err != nil {
		return nil, err
	}
	return c.readResp(r, true, "FOUND %d", &id)
}

func (c *Conn) Peek(id uint64) (body []byte, err error) {
	body, err = c.peek(id)

	if err != nil && err.(ConnError).IsEOF() {
		if retryErr := c.Reconnect(); retryErr == nil {
			return c.Peek(id)
		}
	}
	return
}

// Stats retrieves global statistics from the server.
func (c *Conn) stats() (map[string]string, error) {
	r, err := c.cmd(nil, nil, nil, "stats")
	if err != nil {
		return nil, err
	}
	body, err := c.readResp(r, true, "OK")
	return parseDict(body), err
}

func (c *Conn) Stats() (dict map[string]string, err error) {
	dict, err = c.stats()

	if err != nil && err.(ConnError).IsEOF() {
		if retryErr := c.Reconnect(); retryErr == nil {
			return c.Stats()
		}
	}
	return
}

// StatsJob retrieves statistics about the given job.
func (c *Conn) statsJob(id uint64) (map[string]string, error) {
	r, err := c.cmd(nil, nil, nil, "stats-job", id)
	if err != nil {
		return nil, err
	}
	body, err := c.readResp(r, true, "OK")
	return parseDict(body), err
}

func (c *Conn) StatsJob(id uint64) (dict map[string]string, err error) {
	dict, err = c.statsJob(id)

	if err != nil && err.(ConnError).IsEOF() {
		if retryErr := c.Reconnect(); retryErr == nil {
			return c.StatsJob(id)
		}
	}
	return
}

// ListTubes returns the names of the tubes that currently
// exist on the server.
func (c *Conn) listTubes() ([]string, error) {
	r, err := c.cmd(nil, nil, nil, "list-tubes")
	if err != nil {
		return nil, err
	}
	body, err := c.readResp(r, true, "OK")
	return parseList(body), err
}

func (c *Conn) ListTubes() (tubes []string, err error) {
	tubes, err = c.listTubes()

	if err != nil && err.(ConnError).IsEOF() {
		if retryErr := c.Reconnect(); retryErr == nil {
			return c.ListTubes()
		}
	}
	return
}

func scan(input, format string, a ...interface{}) error {
	_, err := fmt.Sscanf(input, format, a...)
	if err != nil {
		return findRespError(input)
	}
	return nil
}

type req struct {
	id uint
	op string
}
