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

package commands

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"github.com/heketi/heketi/apps/glusterfs"
	"github.com/heketi/heketi/utils"
	"github.com/lpabon/godbc"
	"net/http"
)

type CreateNewClusterCommand struct {
	Cmd
	options *Options
}

func NewCreateNewClusterCommand(options *Options) *CreateNewClusterCommand {

	godbc.Require(options != nil)
	godbc.Require(options.Url != "")

	cmd := &CreateNewClusterCommand{}
	cmd.name = "create"
	cmd.options = options
	cmd.flags = flag.NewFlagSet(cmd.name, flag.ExitOnError)

	//usage on -help
	cmd.flags.Usage = func() {
		fmt.Println(usageTemplateClusterCreate)
	}

	godbc.Ensure(cmd.flags != nil)
	godbc.Ensure(cmd.name == "create")

	return cmd
}

func (a *CreateNewClusterCommand) Name() string {
	return a.name

}

func (a *CreateNewClusterCommand) Exec(args []string) error {

	//parse args
	a.flags.Parse(args)

	s := a.flags.Args()
	//ensure length
	if len(s) > 0 {
		return errors.New("Too many arguments!")
	}

	//set url
	url := a.options.Url

	//do http POST and check if sent to server
	r, err := http.Post(url+"/clusters", "application/json", bytes.NewBuffer([]byte("{}")))
	if err != nil {
		fmt.Fprintf(stdout, "Error: Unable to send command to server: %v", err)
		return err
	}

	//check status code
	if r.StatusCode != http.StatusCreated {
		s, err := utils.GetStringFromResponse(r)
		if err != nil {
			return err
		}
		return errors.New(s)
	}

	//check json response
	var body glusterfs.ClusterInfoResponse
	err = utils.GetJsonFromResponse(r, &body)
	if err != nil {
		fmt.Println("Error: Bad json response from server")
		return err
	}

	//if all is well, print stuff
	fmt.Fprintf(stdout, "Cluster id: %v", body.Id)

	return nil

}
