package cog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"time"
)

type (
	header uint64

	Priority int
	Option   string

	Server struct {
		sync.Mutex
		Conn net.Conn
	}
)

const (
	// []byte{0, 'R', 'E', 'Q', 0, 0, 0, 0}
	REQ (header) = 'R'<<56 | 'E'<<48 | 'Q'<<40
	// []byte{0, 'R', 'E', 'S', 0, 0, 0, 0}
	RES (header) = 'R'<<56 | 'E'<<48 | 'S'<<40

	// worker -> server
	WS_CAN_DO          = REQ | 1
	WS_CAN_DO_TIMEOUT  = REQ | 23
	WS_CANT_DO         = REQ | 2
	WS_RESET_ABILITIES = REQ | 3
	WS_PRE_SLEEP       = REQ | 4
	WS_SET_CLIENT_ID   = REQ | 22
	WS_ALL_YOURS       = REQ | 24
	WS_ECHO_REQ        = REQ | 16
	WS_OPTION_REQ      = REQ | 26
	WS_GRAB_JOB        = REQ | 9
	WS_GRAB_JOB_UNIQ   = REQ | 30
	WS_WORK_STATUS     = REQ | 12
	WS_WORK_COMPLETE   = REQ | 13
	WS_WORK_FAIL       = REQ | 14
	WS_WORK_EXCEPTION  = REQ | 25
	WS_WORK_DATA       = REQ | 28
	WS_WORK_WARNING    = REQ | 29

	// server -> worker
	SW_NOOP            = RES | 6
	SW_NO_JOB          = RES | 10
	SW_JOB_ASSIGN      = RES | 11
	SW_ECHO_RES        = RES | 17
	SW_ERROR           = RES | 19
	SW_OPTION_RES      = RES | 27
	SW_JOB_ASSIGN_UNIQ = RES | 31

	// client -> server
	CS_GET_STATUS         = REQ | 15
	CS_ECHO_REQ           = REQ | 16
	CS_OPTION_REQ         = REQ | 26
	CS_SUBMIT_JOB         = REQ | 7
	CS_SUBMIT_JOB_BG      = REQ | 18
	CS_SUBMIT_JOB_HIGH    = REQ | 21
	CS_SUBMIT_JOB_HIGH_BG = REQ | 32
	CS_SUBMIT_JOB_LOW     = REQ | 33
	CS_SUBMIT_JOB_LOW_BG  = REQ | 34
	CS_SUBMIT_JOB_SCHED   = REQ | 35
	CS_SUBMIT_JOB_EPOCH   = REQ | 36

	// server -> client
	SC_STATUS_RES      = RES | 20
	SC_ECHO_RES        = RES | 17
	SC_OPTION_RES      = RES | 27
	SC_JOB_CREATED     = RES | 8
	SC_ERROR           = RES | 19
	SC_SUBMIT_JOB_HIGH = RES | 21
	SC_WORK_STATUS     = RES | 12
	SC_WORK_COMPLETE   = RES | 13
	SC_WORK_FAIL       = RES | 14
	SC_WORK_DATA       = RES | 28
	SC_WORK_WARNING    = RES | 29
)

var (
	DialTimeout = 5 * time.Second
	RWTimeout   = 30 * time.Second

	nilBytes = []byte{}
)

func NewServer(addr string) (*Server, error) {

	conn, err := net.DialTimeout("tcp", addr, DialTimeout)
	if err != nil {
		return nil, err
	}

	return &Server{
		Conn: conn,
	}, nil

}

func (s *Server) Send(h header, data []byte) error {
	s.Lock()
	defer s.Unlock()
	return s.send(h, data)
}

func (s *Server) Receive() (rh header, rdata []byte, err error) {
	s.Lock()
	defer s.Unlock()
	return s.receive()
}

func (s *Server) SendAndReceive(h header, data []byte) (header, []byte, error) {
	s.Lock()
	defer s.Unlock()

	if err := s.send(h, data); err != nil {
		return 0, nilBytes, err
	}

	return s.receive()
}

func (s *Server) send(h header, data []byte) error {

	// write header
	if err := binary.Write(s.Conn, binary.BigEndian, h); err != nil {
		return err
	}

	// write datalen
	if err := binary.Write(s.Conn, binary.BigEndian, uint16(len(data))); err != nil {
		return err
	}

	// write data
	if len(data) > 0 {
		n, err := s.Conn.Write(data)
		if err != nil {
			return err
		}
		if n != len(data) {
			return errors.New("short write")
		}
	}

	return nil

}

func (s *Server) receive() (header, []byte, error) {

	var (
		buf     = bufio.NewReader(s.Conn)
		res     header
		datalen uint16
	)

	if err := binary.Read(buf, binary.BigEndian, &res); err != nil {
		return 0, nilBytes, err
	}

	if err := binary.Read(buf, binary.BigEndian, &datalen); err != nil {
		return 0, nilBytes, err
	}

	if datalen > 0 {

		rdata := make([]byte, int(datalen))

		n, err := buf.Read(rdata)
		if err != nil {
			return 0, nilBytes, err
		}
		if n != int(datalen) {
			return 0, nilBytes, errors.New("shorty shit")
		}

		return res, rdata, nil

	}

	return res, nilBytes, nil

}
