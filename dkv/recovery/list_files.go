package recovery

import (
	"encoding/json"
	"io"
)

func ListFiles(reader io.Reader) ([]string, error) {
	// Decode reader data into checkpoint list JSON document
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	listDoc := checkpointListDocument{}
	if err := json.Unmarshal(data, &listDoc); err != nil {
		return nil, err
	}

	// Always use the latest checkpoint
	ckpt := listDoc.Checkpoints[len(listDoc.Checkpoints)-1]

	fileNames := []string{}

	// Add WAL file names to output
	for _, w := range ckpt.WALs {
		fileNames = append(fileNames, w.URI)
	}

	// Add all table names to the output
	for _, level := range ckpt.Levels {
		for _, table := range level {
			fileNames = append(fileNames, table.Name)
		}
	}

	return fileNames, nil
}
