package jobqueue

import (
	"io"
	"net/http"
	"os"
	"strconv"
)

func archiveFile(linkData *LinkCopyData) (string, error) {
	archiver_client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}}

	resp, err := archiver_client.Get(linkData.Link)

	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	file_name := strconv.FormatUint(linkData.Id, 10)

	local_archive, err := os.Create(file_name)
	if err != nil {
		return "", err
	}
	defer local_archive.Close()
	defer os.Remove(file_name)

	_, err = io.Copy(local_archive, resp.Body)
	if err != nil {
		return "", err
	}

	// Copying the file to a local location before copying to the shared volume,
	// in case I want to replace a shared volume with a external file store later
	archive_file, err := os.Create("archive/" + file_name)
	if err != nil {
		return "", err
	}
	local_archive.Seek(0, io.SeekStart)

	_, err = io.Copy(archive_file, local_archive)
	if err != nil {
		os.Remove("archive/" + file_name)
	}
	archive_file.Close()
	return "archive/" + file_name, err
}
