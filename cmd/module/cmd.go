// This package provides the entrypoint for the module
package main

import (
	"context"

	cam "github.com/viam-modules/video-store/model/camera"
	vsutils "github.com/viam-modules/video-store/videostore/utils"
	"go.viam.com/rdk/components/camera"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/module"
	"go.viam.com/utils"
)

func main() {
	utils.ContextualMain(mainWithArgs, module.NewLoggerFromArgs("[video-store-module]"))
}

func mainWithArgs(ctx context.Context, _ []string, logger logging.Logger) error {
	if logger.GetLevel() == logging.DEBUG {
		vsutils.SetLibAVLogLevel("debug")
	} else {
		vsutils.SetLibAVLogLevel("error")
	}
	vsutils.SetFFmpegLogCallback()

	module, err := module.NewModuleFromArgs(ctx)
	if err != nil {
		return err
	}
	err = module.AddModelFromRegistry(ctx, camera.API, cam.Model)
	if err != nil {
		return err
	}
	err = module.Start(ctx)
	defer module.Close(ctx)
	if err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}
