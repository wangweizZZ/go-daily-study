package http

import (
	"fmt"
	"net/http"
	"strings"
	"wangweizZZ/kv/pkg/bitcask"
)

const favicon = "favicon.ico"

func InternalError(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "500 Server error", http.StatusInternalServerError)
}
func BadRequest(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "400 bad request", http.StatusBadRequest)
}
func Success(w http.ResponseWriter, result string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, result)
}

type server struct {
	store *bitcask.Bitcask
}

func NewServer(dir string) (*server, error) {
	s, err := bitcask.Open(dir)
	if err != nil {
		return nil, err
	}
	return &server{
		store: s,
	}, nil
}

func (m *handleDispatcher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			InternalError(w, r)
		}
	}()

	if r.URL.Path[1:] == favicon {
		return
	}
	parts := strings.SplitN(r.URL.Path[1:], "/", 2)
	if len(parts) == 0 {
		BadRequest(w, r)
		return
	}
	action := strings.ToUpper(parts[0])
	if _, ok := m.handlers[action]; !ok {
		http.NotFound(w, r)
		return
	}
	m.handlers[action].ServeHTTP(w, r)
}

func (s *server) Serve() error {
	http.Handle("/", newKVDispatcher(s))
	return http.ListenAndServe(":8080", nil)
}

type handleDispatcher struct {
	handlers map[string]http.HandlerFunc
}

func newKVDispatcher(s *server) *handleDispatcher {
	hs := make(map[string]http.HandlerFunc)

	setFunc, getFunc, delFunc, listFunc :=
		func(w http.ResponseWriter, r *http.Request) {
			parts := strings.SplitN(r.URL.Path[1:], "/", 3)
			if len(parts) != 3 {
				BadRequest(w, r)
			}
			err := s.store.Put(parts[1], parts[2])
			if err != nil {
				InternalError(w, r)
			} else {
				Success(w, "OK")
			}
		},
		func(w http.ResponseWriter, r *http.Request) {
			parts := strings.SplitN(r.URL.Path[1:], "/", 2)
			if len(parts) != 2 {
				BadRequest(w, r)
			}
			res, ok, err := s.store.Get(parts[1])
			switch {
			case err != nil:
				InternalError(w, r)
			case !ok:
				Success(w, "")
			default:
				Success(w, res)
			}
		},
		func(w http.ResponseWriter, r *http.Request) {
			parts := strings.SplitN(r.URL.Path[1:], "/", 2)
			if len(parts) != 2 {
				BadRequest(w, r)
			}
			err := s.store.Delete(parts[1])
			if err != nil {
				InternalError(w, r)
			} else {
				Success(w, "OK")
			}
		}, func(w http.ResponseWriter, r *http.Request) {
			Success(w, strings.Join(s.store.List(), ";"))
		}

	hs["SET"] = setFunc
	hs["GET"] = getFunc
	hs["DEL"] = delFunc
	hs["LIST"] = listFunc
	hs["COMMAND"] = func(w http.ResponseWriter, r *http.Request) {
		Success(w, "SET,GET,DEL,LIST")
	}
	return &handleDispatcher{
		handlers: hs,
	}
}
