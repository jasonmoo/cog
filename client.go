package cog

import (
	"bytes"
	"errors"
	"strconv"
	"time"
)

type (
	Client struct {
		server *Server
	}
)

const (
	LowPriority Priority = iota - 1
	NormalPriority
	HighPriority

	OptionExceptions Option = "exceptions"
)

func NewClient(addr string) (*Client, error) {

	s, err := NewServer(addr)
	if err != nil {
		return nil, err
	}

	return &Client{server: s}, nil

}

// A client issues one of these when a job needs to be run. The
// server will then assign a job handle and respond with a JOB_CREATED
// packet.
//
// If one of the BG versions is used, the client is not updated with
// status or notified when the job has completed (it is detached).
//
// The Gearman job server queue is implemented with three levels:
// normal, high, and low. Jobs submitted with one of the HIGH versions
// always take precedence, and jobs submitted with the normal versions
// take precedence over the LOW versions.
//
// Arguments:
// - NULL byte terminated function name.
// - NULL byte terminated unique ID.
// - Opaque data that is given to the function as an argument.
func (c *Client) SubmitJob(func_name, uniq string, data []byte, p Priority) (job_handle string, err error) {

	var h header

	switch p {
	case LowPriority:
		h = CS_SUBMIT_JOB_LOW
	case NormalPriority:
		h = CS_SUBMIT_JOB
	case HighPriority:
		h = CS_SUBMIT_JOB_HIGH
	}

	buf := new(bytes.Buffer)
	buf.WriteString(func_name)
	buf.WriteByte(0)
	buf.WriteString(uniq)
	buf.WriteByte(0)
	buf.Write(data)

	res, rdata, err := c.server.SendAndReceive(h, buf.Bytes())
	if err != nil {
		return "", err
	}
	if res == SC_JOB_CREATED {
		return string(rdata), nil
	}

	return "", errors.New("fwf gereamaNNuh")

}

func (c *Client) SubmitBackgroundJob(func_name, uniq string, data []byte, p Priority) error {

	var h header

	switch p {
	case LowPriority:
		h = CS_SUBMIT_JOB_LOW_BG
	case NormalPriority:
		h = CS_SUBMIT_JOB_BG
	case HighPriority:
		h = CS_SUBMIT_JOB_HIGH_BG
	}

	buf := new(bytes.Buffer)
	buf.WriteString(func_name)
	buf.WriteByte(0)
	buf.WriteString(uniq)
	buf.WriteByte(0)
	buf.Write(data)

	return c.server.Send(h, buf.Bytes())

}

// Just like SUBMIT_JOB_BG, but run job at given time instead of
// immediately. This is not currently used and may be removed.
//
// Arguments:
// - NULL byte terminated function name.
// - NULL byte terminated unique ID.
// - NULL byte terminated minute (0-59).
// - NULL byte terminated hour (0-23).
// - NULL byte terminated day of month (1-31).
// - NULL byte terminated month (1-12).
// - NULL byte terminated day of week (0-6, 0 = Monday).
// - Opaque data that is given to the function as an argument.
func (c *Client) SubmitScheduledJob(func_name, uniq string, t time.Time, data []byte) error {

	weekday := int(t.Weekday())

	// go week day number starts at 0 on Sunday,
	// gearman starts at 0 on Monday
	if weekday == 0 {
		weekday = 7
	}
	weekday -= 1

	buf := new(bytes.Buffer)
	buf.WriteString(func_name)
	buf.WriteByte(0)
	buf.WriteString(uniq)
	buf.WriteByte(0)
	buf.WriteString(strconv.Itoa(t.Minute()))
	buf.WriteByte(0)
	buf.WriteString(strconv.Itoa(t.Hour()))
	buf.WriteByte(0)
	buf.WriteString(strconv.Itoa(t.Day()))
	buf.WriteByte(0)
	buf.WriteString(strconv.Itoa(int(t.Month())))
	buf.WriteByte(0)
	buf.WriteString(strconv.Itoa(weekday))
	buf.WriteByte(0)
	buf.Write(data)

	return c.server.Send(CS_SUBMIT_JOB_EPOCH, buf.Bytes())

}

// Just like SUBMIT_JOB_BG, but run job at given time instead of
// immediately. This is not currently used and may be removed.
//
// Arguments:
// - NULL byte terminated function name.
// - NULL byte terminated unique ID.
// - NULL byte terminated epoch time.
// - Opaque data that is given to the function as an argument.
func (c *Client) SubmitEpochJob(func_name, uniq string, t time.Time, data []byte) {
	panic("unimpleemmtnt")
}

// A client issues this to get status information for a submitted job.
//
// Arguments:
// - Job handle that was given in JOB_CREATED packet.
func (c *Client) GetStatus(job_handle string) error {

	return c.server.Send(CS_GET_STATUS, []byte(job_handle))

}

// A client issues this to set an option for the connection in the
// job server. Returns a OPTION_RES packet on success, or an ERROR
// packet on failure.
//
// Arguments:
// - Name of the option to set. Possibilities are:
//   * "exceptions" - Forward WORK_EXCEPTION packets to the client.
func (c *Client) SetOption(option Option) error {

	h, data, err := c.server.SendAndReceive(CS_OPTION_REQ, []byte(option))
	if err != nil {
		return err
	}

	switch h {
	case SC_OPTION_RES:
		return nil
	case SC_ERROR:
		return errors.New(string(bytes.Replace(data, []byte{0}, []byte(": "), 1)))
	}

	return errors.New("wtf geremans")
}
