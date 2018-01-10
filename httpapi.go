package quorum

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/pprof"

	"github.com/julienschmidt/httprouter"
)

type HTTPApi struct {
	quorumNode *QuorumNode
}

func NewHTTPApi(quorumNode *QuorumNode) *HTTPApi {
	return &HTTPApi{quorumNode: quorumNode}
}

func wrapHandler(h http.Handler) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		h.ServeHTTP(w, r)
	}
}

// Register any endpoints to the router
func (h *HTTPApi) Start(router *httprouter.Router) {
	router.POST("/v1/join", h.Join)
	router.POST("/v1/leave", h.Leave)

	// TODO: options to enable/disable (or scope to just localhost)
	router.GET("/v1/debug/pprof/", wrapHandler(http.HandlerFunc(pprof.Index)))
	router.GET("/v1/debug/pprof/cmdline", wrapHandler(http.HandlerFunc(pprof.Cmdline)))
	router.GET("/v1/debug/pprof/profile", wrapHandler(http.HandlerFunc(pprof.Profile)))
	router.GET("/v1/debug/pprof/symbol", wrapHandler(http.HandlerFunc(pprof.Symbol)))
	router.GET("/v1/debug/pprof/trace", wrapHandler(http.HandlerFunc(pprof.Trace)))
}

type JoinRequest struct {
	ServerID      string `json:"server_id"`
	ServerAddress string `json:"server_address"`
}

type JoinResponse struct {
	OK     bool   `json:"ok"`
	Leader string `json:"leader"`
}

type LeaveRequest struct {
	ServerID string `json:"server_id"`
}

type LeaveResponse struct {
	OK bool `json:"ok"`
}

func (h *HTTPApi) Join(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	defer r.Body.Close()
	bytes, _ := ioutil.ReadAll(r.Body)

	var joinRequest JoinRequest

	if err := json.Unmarshal(bytes, &joinRequest); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	} else {
		if joinRequest.ServerID == "" || joinRequest.ServerAddress == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Expecting a payload like {'server_id' : 'server_id_here', 'server_address' : 'server_address_here'}"))
			return
		}
		ok := false
		leader := ""
		if err := h.quorumNode.Join(joinRequest.ServerID, joinRequest.ServerAddress); err != nil {
			if qleader, err := h.quorumNode.GetLeader(); err == nil {
				leader = qleader
			} else {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				return
			}
		} else {
			ok = true
		}
		if returnBytes, err := json.Marshal(JoinResponse{OK: ok, Leader: leader}); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.Write(returnBytes)
		}
	}
}

func (h *HTTPApi) Leave(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	defer r.Body.Close()
	bytes, _ := ioutil.ReadAll(r.Body)

	var leaveRequest LeaveRequest

	if err := json.Unmarshal(bytes, &leaveRequest); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	} else {
		if leaveRequest.ServerID == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Expecting a payload like {'server_id' : 'server_id_here'}"))
			return
		}
		ok := false
		if err := h.quorumNode.Remove(leaveRequest.ServerID); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		} else {
			ok = true
		}
		if returnBytes, err := json.Marshal(LeaveResponse{OK: ok}); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.Write(returnBytes)
		}
	}
}
