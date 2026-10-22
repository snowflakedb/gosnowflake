package gosnowflake

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

const azureGitTestContainer = "git-repositoryzz01234567-89ab-cdef-0123-456789abcdef"
const azureGitTestSAS = "?sv=2023-01-03&sr=c&sp=r&sig=synthetic%2Bsignature%2F%3D"

type azureURLTestTransport func(*http.Request) (*http.Response, error)

func (f azureURLTestTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func azureURLTestInfo(stagePath, location string) execResponseStageInfo {
	return execResponseStageInfo{
		LocationType: "AZURE", Path: stagePath, Location: location,
		StorageAccount: "sanitizedaccount", EndPoint: "blob.core.windows.net",
		Creds: execResponseCredentials{AzureSasToken: azureGitTestSAS},
	}
}

func azureURLTestClient(t *testing.T, util *snowflakeAzureClient, info *execResponseStageInfo) *azblob.Client {
	t.Helper()
	client, err := util.createClient(info, false, &snowflakeTelemetry{})
	if err != nil {
		t.Fatal(err)
	}
	return client.(*azblob.Client)
}

func assertAzureTestURL(t *testing.T, u *url.URL, wantPath, sas string) {
	t.Helper()
	if u.Scheme != "https" || u.Host != "sanitizedaccount.blob.core.windows.net" || u.Path != wantPath {
		t.Errorf("request URL = %s, want https://sanitizedaccount.blob.core.windows.net%s", u, wantPath)
	}
	wantQuery, err := url.ParseQuery(strings.TrimPrefix(sas, "?"))
	if err != nil {
		t.Fatal(err)
	}
	for key, values := range wantQuery {
		if got := u.Query()[key]; len(got) != 1 || got[0] != values[0] {
			t.Errorf("query %s = %v, want %v", key, got, values)
		}
	}
}

func TestAzureGitDownloadURL(t *testing.T) {
	stagePath := azureGitTestContainer + "/blobs/"
	filename := "0123456789abcdef0123456789abcdef01234567/blobs/report #+%2F雪.txt"
	payload := "sanitized Git file content\n"
	for _, mode := range []string{"HEAD", "disk GET", "buffered GET", "streaming GET"} {
		t.Run(mode, func(t *testing.T) {
			var requests atomic.Int32
			util := &snowflakeAzureClient{cfg: &Config{Transporter: azureURLTestTransport(func(req *http.Request) (*http.Response, error) {
				requests.Add(1)
				assertAzureTestURL(t, req.URL, "/"+stagePath+filename, azureGitTestSAS)
				if req.Method != http.MethodHead && req.Method != http.MethodGet {
					return nil, fmt.Errorf("unexpected method %s", req.Method)
				}
				header := http.Header{"Content-Length": {strconv.Itoa(len(payload))}, "Etag": {"\"test-etag\""}}
				status, body := http.StatusOK, payload
				if req.Method == http.MethodHead {
					body = ""
				} else if req.Header.Get("x-ms-range") != "" || req.Header.Get("Range") != "" {
					status = http.StatusPartialContent
					header.Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(payload)-1, len(payload)))
				}
				return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(payload)), Request: req}, nil
			})}}
			info := azureURLTestInfo(stagePath, stagePath)
			meta := fileMetadata{stageInfo: &info, srcFileName: filename, dstStream: &bytes.Buffer{}, client: azureURLTestClient(t, util, &info)}
			var got []byte
			var err error
			switch mode {
			case "HEAD":
				_, err = util.getFileHeader(context.Background(), &meta, filename)
			case "disk GET":
				destination := filepath.Join(t.TempDir(), "download.txt")
				err = util.nativeDownloadFile(context.Background(), &meta, destination, 1, 0)
				if err == nil {
					got, err = os.ReadFile(destination)
				}
			case "buffered GET":
				err = util.nativeDownloadFile(WithFileGetStream(context.Background(), meta.dstStream), &meta, "", 1, 0)
				got = meta.dstStream.Bytes()
			case "streaming GET":
				var reader io.ReadCloser
				reader, err = util.downloadToStream(context.Background(), &meta)
				if err == nil {
					got, err = io.ReadAll(reader)
					if closeErr := reader.Close(); closeErr != nil {
						t.Fatal(closeErr)
					}
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if requests.Load() == 0 {
				t.Fatal("Azure SDK did not make a request")
			}
			if mode != "HEAD" && string(got) != payload {
				t.Errorf("downloaded %q, want %q", got, payload)
			}
			if info.Path != stagePath || info.Location != stagePath {
				t.Fatal("client creation changed stage metadata")
			}
		})
	}
}

func TestAzureStageClientURLCompatibility(t *testing.T) {
	gitPath := azureGitTestContainer + "/blobs/"
	for _, tc := range []struct{ name, stagePath, location, wantPath string }{
		{"Git repository", gitPath, gitPath, azureGitTestContainer + "/"},
		{"ordinary blobs folder", "ordinary/blobs/", "ordinary/blobs/", "ordinary/blobs/"},
		{"malformed Git name", "git-repositoryzznot-a-uuid/blobs/", "git-repositoryzznot-a-uuid/blobs/", "git-repositoryzznot-a-uuid/blobs/"},
		{"mismatched metadata", gitPath, azureGitTestContainer + "/other/", gitPath},
		{"different prefix", azureGitTestContainer + "/other/", azureGitTestContainer + "/other/", azureGitTestContainer + "/other/"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			util := &snowflakeAzureClient{cfg: &Config{Transporter: azureURLTestTransport(func(*http.Request) (*http.Response, error) {
				return nil, fmt.Errorf("client creation must not make a request")
			})}}
			info := azureURLTestInfo(tc.stagePath, tc.location)
			for _, sas := range []string{azureGitTestSAS, "?sv=2023-01-03&sr=c&sp=r&sig=renewed%2Bsignature%3D"} {
				info.Creds.AzureSasToken = sas
				clientURL, err := url.Parse(azureURLTestClient(t, util, &info).URL())
				if err != nil {
					t.Fatal(err)
				}
				assertAzureTestURL(t, clientURL, "/"+tc.wantPath, sas)
				if clientURL.RawQuery != strings.TrimPrefix(sas, "?") {
					t.Errorf("client SAS query = %q, want original bytes %q", clientURL.RawQuery, strings.TrimPrefix(sas, "?"))
				}
				if info.Path != tc.stagePath || info.Location != tc.location {
					t.Fatal("client creation changed stage metadata")
				}
			}
		})
	}
}

func TestAzureStageUploadURLCompatibility(t *testing.T) {
	payload := "ordinary stage upload\n"
	filename := "report #+%.txt"
	var requests atomic.Int32
	util := &snowflakeAzureClient{cfg: &Config{Transporter: azureURLTestTransport(func(req *http.Request) (*http.Response, error) {
		requests.Add(1)
		assertAzureTestURL(t, req.URL, "/ordinary/prefix/blobs/"+filename, azureGitTestSAS)
		if req.Method != http.MethodPut {
			return nil, fmt.Errorf("unexpected method %s", req.Method)
		}
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		if string(body) != payload {
			t.Errorf("uploaded %q, want %q", body, payload)
		}
		return &http.Response{StatusCode: http.StatusCreated, Header: http.Header{"Etag": {"\"test-etag\""}}, Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
	})}}
	info := azureURLTestInfo("ordinary/", "ordinary/prefix/blobs/")
	meta := fileMetadata{stageInfo: &info, dstFileName: filename, uploadSize: int64(len(payload)), options: &SnowflakeFileTransferOptions{}, client: azureURLTestClient(t, util, &info)}
	source := filepath.Join(t.TempDir(), "upload.txt")
	if err := os.WriteFile(source, []byte(payload), 0600); err != nil {
		t.Fatal(err)
	}
	if err := util.uploadFile(context.Background(), source, &meta, 1, 0); err != nil {
		t.Fatal(err)
	}
	if requests.Load() == 0 || meta.resStatus != uploaded {
		t.Fatal("Azure SDK did not complete the upload")
	}
}
