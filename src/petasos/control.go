package main

import (
	"fmt"
	"github.com/Comcast/webpa-common/concurrent"
	"github.com/Comcast/webpa-common/logging"
	"github.com/Comcast/webpa-common/logging/logginghttp"
	"github.com/Comcast/webpa-common/server"
	"github.com/Comcast/webpa-common/xhttp"
	"github.com/Comcast/webpa-common/xhttp/gate"
	"github.com/Comcast/webpa-common/xhttp/xcontext"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gorilla/mux"
	"github.com/spf13/viper"
)

const (
	ControlKey = "control"
)

func StartControlServer(logger log.Logger, regions map[string]gate.Interface, v *viper.Viper, webPA *server.WebPA) error {
	if !v.IsSet(ControlKey) {
		logger.Log(level.Key(), level.ErrorValue(), logging.MessageKey(), "No ControlKey")
		return nil
	}

	var options xhttp.ServerOptions
	if err := v.UnmarshalKey(ControlKey, &options); err != nil {
		logger.Log(level.Key(), level.ErrorValue(), logging.MessageKey(), "Unable to start control server", logging.ErrorKey(), err)
		return err
	}

	options.Logger = logger

	var (
		r          = mux.NewRouter()
		apiHandler = r.PathPrefix(fmt.Sprintf("%s/%s", baseURI, version)).Subrouter()
	)

	for region, g := range regions {
		path := "/" + region + "/gate"
		apiHandler.Handle(path, &gate.Lever{Gate: g, Parameter: "open"}).
			Methods("POST", "PUT", "PATCH")
		apiHandler.Handle(path, &gate.Status{Gate: g}).
			Methods("GET")
	}

	server := xhttp.NewServer(options)
	server.Handler = xcontext.Populate(0, logginghttp.SetLogger(logger))(r)

	starter := xhttp.NewStarter(options.StartOptions(), server)
	go func() {
		if err := starter(); err != nil {
			logger.Log(level.Key(), level.ErrorValue(), logging.MessageKey(), "Unable to start control server", logging.ErrorKey(), err)
		}

		temp, err := webPA.Metric.NewRegistry()
		_, controlServer, _ := webPA.Prepare(logger, nil, temp, server.Handler)
		_, _, err = concurrent.Execute(controlServer)
		if err != nil {
			logging.Error(logger, logging.MessageKey(), "Unable to start petasos", logging.ErrorKey(), err)
		}
	}()

	return nil
}
