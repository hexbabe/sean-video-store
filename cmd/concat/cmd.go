// package main concatinates video files in a storage directory
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/viam-modules/video-store/model/camera"
	"github.com/viam-modules/video-store/videostore"
	vsutils "github.com/viam-modules/video-store/videostore/utils"
	"go.viam.com/rdk/logging"
)

func main() {
	vsutils.SetLibAVLogLevel("debug")
	ctx := context.Background()
	//nolint:mnd
	if len(os.Args) != 3 {
		//nolint:forbidigo
		fmt.Printf("usage: %s <fetch command json file> <resource name>\n", os.Args[0])
		os.Exit(1)
	}
	logger := logging.NewLogger(os.Args[0])
	c := camera.Config{Storage: camera.Storage{SizeGB: 1}}
	b, err := os.ReadFile(os.Args[1])
	if err != nil {
		logger.Fatal(err.Error())
	}
	rawReq := map[string]interface{}{}
	if err := json.Unmarshal(b, &rawReq); err != nil {
		logger.Fatal(err.Error())
	}

	config, err := camera.ToFrameVideoStoreVideoConfig(&c, os.Args[2], nil)
	if err != nil {
		logger.Fatal(err.Error())
	}
	vs, err := videostore.NewRTPVideoStore(config, logger)
	if err != nil {
		logger.Fatal(err.Error())
	}

	req, err := camera.ToFetchCommand(rawReq)
	if err != nil {
		logger.Fatal(err.Error())
	}
	res, err := vs.Fetch(ctx, req)
	if err != nil {
		logger.Fatal(err.Error())
	}

	//nolint:mnd
	if err := os.WriteFile(os.Args[2]+".mp4", res.Video, 0o600); err != nil {
		logger.Fatal(err.Error())
	}
}
