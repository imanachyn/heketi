package cmdexec

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/heketi/heketi/executors"

	"github.com/lpabon/godbc"
)

const (
	removeBrickStatusInProgress = "in progress"
	removeBrickStatusCompleted  = "completed"
	removeBrickStatusFailed     = "failed"

	removeBrickStatusPollInterval  = 5 * time.Second
	removeBrickStatusRetryInterval = 20 * time.Second
	removeBrickStatusRetryAttempts = 30
)

func (s *CmdExecutor) VolumeAddBrick(host string, volume string, brick *executors.BrickInfo) error {
	godbc.Require(volume != "")
	godbc.Require(host != "")
	godbc.Require(brick != nil)

	// Add the brick
	command := []string{
		fmt.Sprintf("gluster --mode=script volume add-brick %v %v:%v", volume, brick.Host, brick.Path),
	}

	_, err := s.RemoteExecutor.RemoteCommandExecute(host, command, 10)
	if err != nil {
		return logger.Err(fmt.Errorf("unable to add brick %v:%v to volume %v: %v", brick.Host, brick.Path, volume, err))
	}

	return nil
}

func (s *CmdExecutor) VolumeRemoveBrick(host string, volume string, brick *executors.BrickInfo, replica int) error {
	godbc.Require(volume != "")
	godbc.Require(host != "")
	godbc.Require(brick != nil)

	command := fmt.Sprintf("gluster --mode=script volume remove-brick %s ", volume)
	if replica > 0 {
		// Force remove
		command += fmt.Sprintf("replica %d %v:%v force", replica, brick.Host, brick.Path) // Migration of data is not needed when reducing replica count
		commands := []string{command}
		_, err := s.RemoteExecutor.RemoteCommandExecute(host, commands, 10)
		if err != nil {
			return logger.Err(fmt.Errorf("unable to force remove brick %v:%v from volume %v: %v", brick.Host, brick.Path, volume, err))
		}
	} else {
		// Remove with rebalance
		command += fmt.Sprintf("%v:%v ", brick.Host, brick.Path)

		// Start
		commands := []string{command + "start"}
		_, err := s.RemoteExecutor.RemoteCommandExecute(host, commands, 10)
		if err != nil {
			return logger.Err(fmt.Errorf("unable to start remove brick %v:%v from volume %v: %v", brick.Host, brick.Path, volume, err))
		}

		// Status
		status, err := s.waitVolumeRemoveBrickStatus(host, command)
		if err != nil {
			return err
		}
		if status == removeBrickStatusFailed {
			return logger.Err(fmt.Errorf("failed to remove brick %v:%v from volume %v: %v", brick.Host, brick.Path, volume, err))
		}

		// Commit
		commands = []string{command + "commit"}
		_, err = s.RemoteExecutor.RemoteCommandExecute(host, commands, 10)
		if err != nil {
			return logger.Err(fmt.Errorf("unable to commit remove brick %v:%v from volume %v: %v", brick.Host, brick.Path, volume, err))
		}
	}

	return nil
}

func (s *CmdExecutor) waitVolumeRemoveBrickStatus(host string, command string) (string, error) {
	// Status
	commands := []string{command + "status"}
	for {
		var b []string
		var err error
		for i := 1; i <= removeBrickStatusRetryAttempts; i++ {
			b, err = s.RemoteExecutor.RemoteCommandExecute(host, commands, 10)
			if err == nil {
				break
			}
			time.Sleep(removeBrickStatusRetryInterval)
		}
		if err != nil {
			return "", err
		}

		if strings.Contains(b[0], removeBrickStatusInProgress) {
			time.Sleep(removeBrickStatusPollInterval)
		} else if strings.Contains(b[0], removeBrickStatusCompleted) {
			return removeBrickStatusCompleted, nil
		} else if strings.Contains(b[0], removeBrickStatusFailed) {
			return removeBrickStatusFailed, nil
		} else {
			return "", errors.New("unknown status")
		}
	}
}
