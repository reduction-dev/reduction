package snapshots

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"path"
	"path/filepath"

	"reduction.dev/reduction/dkv/recovery"
	"reduction.dev/reduction/proto/snapshotpb"
	"reduction.dev/reduction/storage"
)

// Create a directory with savepoint files based on the provided checkpoint.
func CreateSavepointArtifact(fs storage.FileStore, savepointsPath string, checkpointURI string, snapshot *jobSnapshot) (string, error) {
	// Read the checkpoint data of every operator checkpoint and copy the
	// referenced files into a savepoint directory.
	for _, opCkpt := range snapshot.operatorCheckpoints {
		checkpointsData, err := fs.Read(opCkpt.DkvFileUri)
		if err != nil {
			return "", err
		}

		// Get a list of URIs that the checkpoint references.
		files, err := recovery.ListFiles(bytes.NewBuffer(checkpointsData))
		if err != nil {
			return "", err
		}
		files = append(files, opCkpt.DkvFileUri) // include checkpoints uri

		for _, file := range files {
			opID, baseFileName, err := parseDKVURI(file)
			if err != nil {
				return "", fmt.Errorf("parsing uri while creating savepoint: %v", err)
			}

			// Destination looks like ./savepoints/<snap-id>/dkv/<op-id>/000000.wal
			dst := filepath.Join(savepointsPath, pathSegment(snapshot.id), "dkv", opID, baseFileName)
			if err := fs.Copy(file, dst); err != nil {
				return "", err
			}
		}
	}

	// Copy the job checkpoint file to a job savepoint file.
	savepointDestination := filepath.Join(savepointsPath, pathSegment(snapshot.id), "job.savepoint")
	if err := fs.Copy(checkpointURI, savepointDestination); err != nil {
		return "", fmt.Errorf("failed copying checkpoint to savepoint storage: %v", err)
	}

	return savepointDestination, nil
}

// Copy files from a savepoint into place to be read as checkpoints.
func RestoreFromSavepointArtifact(fs storage.FileStore, savepointURI string, jobCheckpoint *snapshotpb.JobCheckpoint) error {
	spDir := filepath.Dir(savepointURI)

	for _, opCkpt := range jobCheckpoint.GetOperatorCheckpoints() {
		opID, baseFileName, err := parseDKVURI(opCkpt.DkvFileUri)
		if err != nil {
			return fmt.Errorf("restore from savepoint: %v", err)
		}

		// Read the checkpoints data
		checkpointsPath := filepath.Join(spDir, "dkv", opID, baseFileName)
		cpData, err := fs.Read(checkpointsPath)
		if err != nil {
			return fmt.Errorf("reading checkpoints file (%s): %v", checkpointsPath, err)
		}

		// Get a list of referenced files to copy into place
		files, err := recovery.ListFiles(bytes.NewBuffer(cpData))
		if err != nil {
			return err
		}
		files = append(files, opCkpt.DkvFileUri) // Include DKV checkpoints file

		for _, file := range files {
			opPrefix, baseFileName, err := parseDKVURI(file)
			if err != nil {
				return fmt.Errorf("restore savepoint dkv uri: %v", file)
			}

			// Source is a file referenced by a DKV checkpoint that's been copied to the savepoint directory
			src := filepath.Join(spDir, "dkv", opPrefix, baseFileName)
			if err := fs.Copy(src, file); err != nil {
				return err
			}
		}
	}

	return nil
}

// Expect all URIs to end with an operator id followed by the specific file name.
func parseDKVURI(uri string) (opID, base string, err error) {
	opPrefix, baseFileName := path.Split(uri)
	if opPrefix == "" || baseFileName == "" {
		return "", "", fmt.Errorf("file URIs in a DKV checkpoint must contain an operator prefix but was %s", uri)
	}
	return opPrefix, baseFileName, nil
}

// Create a URL safe encoding to create a path segment. Lexicographic order will
// be descending such that later checkpoints will appear first in a file list.
func pathSegment(id uint64) string {
	reversed := math.MaxUint64 - id
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, reversed)
	return base64.RawURLEncoding.EncodeToString(buf)
}
