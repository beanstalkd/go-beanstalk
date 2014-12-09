package beanstalk

import (
	"time"
)

// Tube represents tube Name on the server connected to by Conn.
// It has methods for commands that operate on a single tube.
type Tube struct {
	Conn *Conn
	Name string
}

// Put puts a job into tube t with priority pri and TTR ttr, and returns
// the id of the newly-created job. If delay is nonzero, the server will
// wait the given amount of time after returning to the client and before
// putting the job into the ready queue.
func (t *Tube) put(body []byte, pri uint32, delay, ttr time.Duration) (id uint64, err error) {
	r, err := t.Conn.cmd(t, nil, body, "put", pri, dur(delay), dur(ttr))
	if err != nil {
		return 0, err
	}
	_, err = t.Conn.readResp(r, false, "INSERTED %d", &id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (t *Tube) Put(body []byte, pri uint32, delay, ttr time.Duration) (id uint64, err error) {
	id, err = t.put(body, pri, delay, ttr)

	if err != nil && err.(ConnError).IsEOF() {
		if retryErr := t.Conn.Reconnect(); retryErr == nil {
			return t.Put(body, pri, delay, ttr)
		}
	}

	return
}

// PeekReady gets a copy of the job at the front of t's ready queue.
func (t *Tube) peekReady() (id uint64, body []byte, err error) {
	r, err := t.Conn.cmd(t, nil, nil, "peek-ready")
	if err != nil {
		return 0, nil, err
	}
	body, err = t.Conn.readResp(r, true, "FOUND %d", &id)
	if err != nil {
		return 0, nil, err
	}
	return id, body, nil
}

func (t *Tube) PeekReady() (id uint64, body []byte, err error) {
	id, body, err = t.peekReady()

	if err != nil && err.(ConnError).IsEOF() {
		if retryErr := t.Conn.Reconnect(); retryErr == nil {
			return t.PeekReady()
		}
	}

	return
}

// PeekDelayed gets a copy of the delayed job that is next to be
// put in t's ready queue.
func (t *Tube) peekDelayed() (id uint64, body []byte, err error) {
	r, err := t.Conn.cmd(t, nil, nil, "peek-delayed")
	if err != nil {
		return 0, nil, err
	}
	body, err = t.Conn.readResp(r, true, "FOUND %d", &id)
	if err != nil {
		return 0, nil, err
	}
	return id, body, nil
}

func (t *Tube) PeekDelayed() (id uint64, body []byte, err error) {
	id, body, err = t.peekDelayed()

	if err != nil && err.(ConnError).IsEOF() {
		if retryErr := t.Conn.Reconnect(); retryErr == nil {
			return t.PeekDelayed()
		}
	}

	return
}

// PeekBuried gets a copy of the job in the holding area that would
// be kicked next by Kick.
func (t *Tube) peekBuried() (id uint64, body []byte, err error) {
	r, err := t.Conn.cmd(t, nil, nil, "peek-buried")
	if err != nil {
		return 0, nil, err
	}
	body, err = t.Conn.readResp(r, true, "FOUND %d", &id)
	if err != nil {
		return 0, nil, err
	}
	return id, body, nil
}

func (t *Tube) PeekBuried() (id uint64, body []byte, err error) {
	id, body, err = t.peekBuried()

	if err != nil && err.(ConnError).IsEOF() {
		if retryErr := t.Conn.Reconnect(); retryErr == nil {
			return t.PeekBuried()
		}
	}

	return
}

// Kick takes up to bound jobs from the holding area and moves them into
// the ready queue, then returns the number of jobs moved. Jobs will be
// taken in the order in which they were last buried.
func (t *Tube) kick(bound int) (n int, err error) {
	r, err := t.Conn.cmd(t, nil, nil, "kick", bound)
	if err != nil {
		return 0, err
	}
	_, err = t.Conn.readResp(r, false, "KICKED %d", &n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (t *Tube) Kick(bound int) (n int, err error) {
	n, err = t.kick(bound)

	if err != nil && err.(ConnError).IsEOF() {
		if retryErr := t.Conn.Reconnect(); retryErr == nil {
			return t.Kick(bound)
		}
	}

	return
}

// Stats retrieves statistics about tube t.
func (t *Tube) stats() (map[string]string, error) {
	r, err := t.Conn.cmd(nil, nil, nil, "stats-tube", t.Name)
	if err != nil {
		return nil, err
	}
	body, err := t.Conn.readResp(r, true, "OK")
	return parseDict(body), err
}

func (t *Tube) Stats() (dict map[string]string, err error) {
	dict, err = t.stats()

	if err != nil && err.(ConnError).IsEOF() {
		if retryErr := t.Conn.Reconnect(); retryErr == nil {
			return t.Stats()
		}
	}

	return
}

// Pause pauses new reservations in t for time d.
func (t *Tube) pause(d time.Duration) error {
	r, err := t.Conn.cmd(nil, nil, nil, "pause-tube", t.Name, dur(d))
	if err != nil {
		return err
	}
	_, err = t.Conn.readResp(r, false, "PAUSED")
	if err != nil {
		return err
	}
	return nil
}

func (t *Tube) Pause(d time.Duration) (err error) {
	err = t.pause(d)

	if err != nil && err.(ConnError).IsEOF() {
		if retryErr := t.Conn.Reconnect(); retryErr == nil {
			return t.Pause(d)
		}
	}

	return
}
