//
// Copyright (c) 2015 The heketi Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package glusterfs

import (
	"fmt"
	"github.com/heketi/heketi/rest"
	"net/http"
)

type App struct {
	hello string
}

func NewApp() *App {
	return &App{}
}

// Interface rest.App
func (a *App) GetRoutes() rest.Routes {

	return rest.Routes{

		// HelloWorld
		rest.Route{"Hello", "GET", "/hello", a.Hello},

		// Cluster
		rest.Route{"ClusterCreate", "POST", "/clusters", a.NotImplemented},
		rest.Route{"ClusterInfo", "GET", "/clusters/{id:[A-Fa-f0-9]+}", a.NotImplemented},
		rest.Route{"ClusterList", "GET", "/clusters", a.NotImplemented},
		rest.Route{"ClusterDelete", "DELETE", "/clusters/{id:[A-Fa-f0-9]+}", a.NotImplemented},

		// Node
		rest.Route{"NodeAdd", "POST", "/nodes", a.NotImplemented},
		rest.Route{"NodeInfo", "GET", "/nodes/{id:[A-Fa-f0-9]+}", a.NotImplemented},
		rest.Route{"NodeDelete", "DELETE", "/nodes/{id:[A-Fa-f0-9]+}", a.NotImplemented},

		// Devices
		rest.Route{"DeviceAdd", "POST", "/devices", a.NotImplemented},
		rest.Route{"DeviceInfo", "GET", "/devices/{id:[A-Fa-f0-9]+}", a.NotImplemented},
		rest.Route{"DeviceDelete", "DELETE", "/devices/{id:[A-Fa-f0-9]+}", a.NotImplemented},

		// Volume
		rest.Route{"VolumeCreate", "POST", "/volumes", a.NotImplemented},
		rest.Route{"VolumeInfo", "GET", "/volumes/{id:[A-Fa-f0-9]+}", a.NotImplemented},
		rest.Route{"VolumeExpand", "POST", "/volumes/{id:[A-Fa-f0-9]+}/expand", a.NotImplemented},
		rest.Route{"VolumeDelete", "DELETE", "/volumes/{id:[A-Fa-f0-9]+}", a.NotImplemented},
		rest.Route{"VolumeList", "GET", "/volumes", a.NotImplemented},
	}
}

func (a *App) Close() {

}

func (a *App) Hello(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "HelloWorld from GlusterFS Application")
}

func (a *App) NotImplemented(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Function not yet supported", http.StatusNotImplemented)
}
