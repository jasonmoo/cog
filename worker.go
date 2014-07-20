package cog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strconv"
	"time"
)

type (
	Worker struct {
		server *Server
	}
)

func NewWorker(addr string) (*Worker, error) {

	s, err := NewServer(addr)
	if err != nil {
		return nil, err
	}

	return &Worker{server: s}, nil

}

// This is sent to notify the server that the worker is able to
// perform the given function. The worker is then put on a list to be
// woken up whenever the job server receives a job for that function.
//
// Arguments:
// - Function name.
func (w *Worker) CanDo(func_name string) error {

	return w.server.Send(WS_CAN_DO, []byte(func_name))

}

// Same as CAN_DO, but with a timeout value on how long the job
// is allowed to run. After the timeout value, the job server will
// mark the job as failed and notify any listening Workers.
//
// Arguments:
// - NULL byte terminated Function name.
// - Timeout value. (uint32 in seconds)
func (w *Worker) CanDoTimeout(func_name string, timeout time.Duration) error {

	buf := new(bytes.Buffer)
	buf.WriteString(func_name)
	buf.WriteByte(0)

	if err := binary.Write(buf, binary.BigEndian, uint32(timeout/time.Second)); err != nil {
		return err
	}

	return w.server.Send(WS_CAN_DO_TIMEOUT, buf.Bytes())

}

// This is sent to notify the server that the worker is no longer
// able to perform the given function.
//
// Arguments:
// - Function name.
func (w *Worker) CantDo(func_name string) error {

	return w.server.Send(WS_CANT_DO, []byte(func_name))

}

// This is sent to notify the server that the worker is no longer
// able to do any functions it previously registered with CAN_DO or
// CAN_DO_TIMEOUT.
//
// Arguments:
// - None.
func (w *Worker) ResetAbilities() error {

	return w.server.Send(WS_RESET_ABILITIES, nilBytes)

}

// This is sent to notify the server that the worker is about to
// sleep, and that it should be woken up with a NOOP packet if a
// job comes in for a function the worker is able to perform.
//
// Arguments:
// - None.
func (w *Worker) PreSleep() error {

	return w.server.Send(WS_PRE_SLEEP, nilBytes)

}

// This is sent to the server to request any available jobs on the
// queue. The server will respond with either NO_JOB or JOB_ASSIGN,
// depending on whether a job is available.

// Arguments:
// - None.
func (w *Worker) GrabJob() (job_handle, func_name string, rdata []byte, err error) {

	res, rdata, err := w.server.SendAndReceive(WS_GRAB_JOB, nilBytes)
	if err != nil {
		return "", "", nilBytes, err
	}

	switch res {
	case SW_JOB_ASSIGN:
		if args := bytes.SplitN(rdata, []byte{0}, 3); len(args) == 3 {
			return string(args[0]), string(args[1]), args[2], nil
		}
	case SW_NO_JOB:
		return "", "", nilBytes, nil
	}

	return "", "", nilBytes, errors.New("bad jeorb")

}

// Just like GRAB_JOB, but return JOB_ASSIGN_UNIQ when there is a job.
//
// Arguments:
// - None.
func (w *Worker) GrabJobUniq() (job_handle, func_name, uniq string, rdata []byte, err error) {

	res, rdata, err := w.server.SendAndReceive(WS_GRAB_JOB_UNIQ, nilBytes)
	if err != nil {
		return "", "", "", nilBytes, err
	}

	switch res {
	case SW_JOB_ASSIGN:
		if args := bytes.SplitN(rdata, []byte{0}, 4); len(args) == 4 {
			return string(args[0]), string(args[1]), string(args[2]), args[3], nil
		}
	case SW_NO_JOB:
		return "", "", "", nilBytes, nil
	}

	return "", "", "", nilBytes, errors.New("bad jeorb")

}

// This is sent to update the Worker with data from a running job. A
// worker should use this when it needs to send updates, send partial
// results, or flush data during long running jobs. It can also be
// used to break up a result so the worker does not need to buffer
// the entire result before sending in a WORK_COMPLETE packet.
//
// Arguments:
// - NULL byte terminated job handle.
// - Opaque data that is returned to the Worker.
func (w *Worker) WorkData(job_handle string, data []byte) error {

	buf := new(bytes.Buffer)
	buf.WriteString(job_handle)
	buf.WriteByte(0)
	buf.Write(data)

	return w.server.Send(WS_WORK_DATA, buf.Bytes())

}

// This is sent to update the Worker with a warning. It acts just
// like a WORK_DATA response, but should be treated as a warning
// instead of normal response data.
//
// Arguments:
// - NULL byte terminated job handle.
// - Opaque data that is returned to the Worker.
func (w *Worker) WorkWarning(job_handle string, data []byte) error {

	buf := new(bytes.Buffer)
	buf.WriteString(job_handle)
	buf.WriteByte(0)
	buf.Write(data)

	return w.server.Send(WS_WORK_WARNING, buf.Bytes())

}

// This is sent to update the server (and any listening Workers)
// of the status of a running job. The worker should send these
// periodically for long running jobs to update the percentage
// complete. The job server should store this information so a Worker
// who issued a background command may retrieve it later with a
// GET_STATUS request.
//
// Arguments:
// - NULL byte terminated job handle.
// - NULL byte terminated percent complete numerator.
// - Percent complete denominator.
func (w *Worker) WorkStatus(job_handle string, n, m int) error {

	buf := new(bytes.Buffer)
	buf.WriteString(job_handle)
	buf.WriteByte(0)
	buf.WriteString(strconv.Itoa(n))
	buf.WriteByte(0)
	buf.WriteString(strconv.Itoa(m))

	return w.server.Send(WS_WORK_WARNING, buf.Bytes())

}

// This is to notify the server (and any listening Workers) that
// the job completed successfully.
//
// Arguments:
// - NULL byte terminated job handle.
// - Opaque data that is returned to the Worker as a response.
func (w *Worker) WorkComplete(job_handle string, data []byte) error {

	buf := new(bytes.Buffer)
	buf.WriteString(job_handle)
	buf.WriteByte(0)
	buf.Write(data)

	return w.server.Send(WS_WORK_COMPLETE, buf.Bytes())

}

// This is to notify the server (and any listening Workers) that
// the job failed.
//
// Arguments:
// - Job handle.
func (w *Worker) WorkFail(job_handle string) error {

	return w.server.Send(WS_WORK_FAIL, []byte(job_handle))

}

// This is to notify the server (and any listening Workers) that
// the job failed with the given exception.
//
// Arguments:
// - NULL byte terminated job handle.
// - Opaque data that is returned to the Worker as an exception.
func (w *Worker) WorkException(job_handle string, data []byte) error {

	buf := new(bytes.Buffer)
	buf.WriteString(job_handle)
	buf.WriteByte(0)
	buf.Write(data)

	return w.server.Send(WS_WORK_EXCEPTION, buf.Bytes())

}

// This sets the worker ID in a job server so monitoring and reporting
// commands can uniquely identify the various workers, and different
// connections to job servers from the same worker.
//
// Arguments:
// - Unique string to identify the worker instance.
func (w *Worker) SetWorkerId(id string) error {

	return w.server.Send(WS_SET_CLIENT_ID, []byte(id))

}

// Not yet implemented. This looks like it is used to notify a job
// server that this is the only job server it is connected to, so
// a job can be given directly to this worker with a JOB_ASSIGN and
// no worker wake-up is required.
//
// Arguments:
// - None.
func (w *Worker) AllYours() {
	panic("unimpleemmtnt")
}

// When a job server receives this request, it simply generates a
// ECHO_RES packet with the data. This is primarily used for testing
// or debugging.
//
// Arguments:
// - Opaque data that is echoed back in response.
func (w *Worker) Echo(str string) error {

	message := []byte(str)

	header, data, err := w.server.SendAndReceive(WS_SET_CLIENT_ID, message)
	if err != nil {
		return err
	}

	if header == SW_ECHO_RES && bytes.Equal(data, message) {
		return nil
	}

	return errors.New("wfh geremans fuck")

}
