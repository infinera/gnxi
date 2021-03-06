/* Copyright 2018 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Binary implements a gNOI Target with a Certificate Management service.
package main

import (
	"flag"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/google/gnxi/gnoi"
	"github.com/google/gnxi/gnoi/cert"
	"github.com/google/gnxi/gnoi/os"
	"github.com/google/gnxi/gnoi/reset"
	"github.com/google/gnxi/utils/credentials"
	"google.golang.org/grpc"

	log "github.com/golang/glog"
)

var (
	gNOIServer    *gnoi.Server
	grpcServer    *grpc.Server
	muServe       sync.Mutex
	bootstrapping bool

	certID               = flag.String("cert_id", "default", "Certificate ID for preloaded certificates")
	bindAddr             = flag.String("bind_address", ":9339", "Bind to address:port or just :port")
	resetDelay           = flag.Duration("reset_delay", 3*time.Second, "Delay before resetting the service upon factory reset request, 3 seconds by default")
	zeroFillUnsupported  = flag.Bool("zero_fill_unsupported", false, "Make the target not support zero filling storage")
	factoryOSUnsupported = flag.Bool("reset_unsupported", false, "Make the target not support factory resetting OS")
	factoryVersion       = flag.String("factoryOS_version", "1.0.0a", "Specify factory OS version, 1.0.0a by default")
	installedVersions    = flag.String("installedOS_versions", "", "Specify installed OS versions, e.g \"1.0.1a 2.01b\"")
	receiveChunkSizeAck  = flag.Uint64("chunk_size_ack", 12000000, "The chunk size of the image to respond with a TransfreResponse in bytes. Example: -chunk_size 12000000")
)

// serve binds to an address and starts serving a gRPCServer.
func serve() {
	muServe.Lock()
	defer muServe.Unlock()
	listen, err := net.Listen("tcp", *bindAddr)
	if err != nil {
		log.Fatal("Failed to listen:", err)
	}
	defer listen.Close()
	log.Info("Starting gNOI server.")
	if err := grpcServer.Serve(listen); err != nil {
		log.Fatal("Failed to serve:", err)
	}
}

// notifyCerts can be called with the number of certs and ca certs installed. It will
// (re)start the gRPC server in encrypted mode if no certs are installed. It will
// (re)start in authenticated mode otherwise.
func notifyCerts(certs, caCerts int) {
	hasCredentials := certs != 0 && caCerts != 0
	if bootstrapping != hasCredentials {
		// Nothing to do, either I am bootstrapping and I have no
		// certificates or I am provisioned and I have certificates.
		return
	}
	if bootstrapping {
		log.Info("Found Credentials, setting Provisioned state.")
		if grpcServer != nil {
			grpcServer.GracefulStop()
		}
		grpcServer = gNOIServer.PrepareAuthenticated()
		// Register all gNOI services.
		gNOIServer.Register(grpcServer)
	} else {
		log.Info("No credentials, setting Bootstrapping state.")
		if grpcServer != nil {
			grpcServer.GracefulStop()
		}
		grpcServer = gNOIServer.PrepareEncrypted()
		// Only register the gNOI Cert service for bootstrapping.
		gNOIServer.RegCertificateManagement(grpcServer)
	}
	bootstrapping = !bootstrapping
	go serve()
}

// start creates the new gNOI server.
func start() {
	resetSettings := &reset.Settings{
		ZeroFillUnsupported:  *zeroFillUnsupported,
		FactoryOSUnsupported: *factoryOSUnsupported,
	}
	osSettings := &os.Settings{
		FactoryVersion:      *factoryVersion,
		InstalledVersions:   strings.Split(*installedVersions, " "),
		ReceiveChunkSizeAck: *receiveChunkSizeAck,
	}
	var (
		numCerts,
		numCA int
		certSettings = &cert.Settings{}
	)
	certSettings.CertID = *certID
	credentials.SetTargetName("target.com")
	certSettings.Cert, certSettings.CA = credentials.ParseCertificates()
	if certSettings.Cert != nil && certSettings.CA != nil {
		numCerts, numCA = 1, 1
	}
	var err error
	if gNOIServer, err = gnoi.NewServer(certSettings, resetSettings, notifyReset, osSettings); err != nil {
		log.Fatal("Failed to create gNOI Server:", err)
	}
	// Registers a caller for whenever the number of installed certificates changes.
	gNOIServer.RegisterCertNotifier(notifyCerts)
	bootstrapping = numCerts != 0 && numCA != 0
	notifyCerts(numCerts, numCA) // Triggers bootstraping mode.
}

// notifyReset is called when the factory reset service requires the server to be restarted.
func notifyReset() {
	log.Info("Server factory reset triggered")
	<-time.After(*resetDelay)
	start()
}

func main() {
	flag.Set("logtostderr", "true")
	flag.Parse()
	start()
	select {} // Loop forever.
}
