package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"

	log "github.com/sirupsen/logrus"
)

var (
	accountKey  string
	accountName string
	endPoint    string
	container   string
	credentials *azblob.SharedKeyCredential
)

func main() {

	if len(os.Args) < 4 {
		_Usage()
		return
	}
	container = os.Args[1]
	ops := os.Args[2]
	var outpath string

	log.SetReportCaller(true)
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true,
		CallerPrettyfier: func(frame *runtime.Frame) (function string, file string) {
			fileName := path.Base(frame.File) + ":" + strconv.Itoa(frame.Line)
			//return frame.Function, fileName
			return frame.Func.Name(), fileName
		}})
	log.Info("azfs commands")

	accountKey, accountName, endPoint, container = GetAccountInfo()

	err := *new(error)
	credentials, err = azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Error("login failed accountname: " + accountName)
	}

	///landing/ran/ericsson/5g/zipped/LBL5ENM1/LBL5ENM1_20210406.zip

	if ops == "-c" {
		blob := os.Args[3]
		if len(os.Args) == 5 {
			outpath = os.Args[4]
		} else {
			outpath = ""
		}
		err := DowloadBlob(blob, outpath)
		if err != nil {
			log.Error("unable to download file: " + err.Error())
		}
	} else if ops == "-u" {
		if len(os.Args) != 5 {
			_Usage()
		}
		localFie := os.Args[3]
		blobLocation := os.Args[4]
		err := UploadFile(localFie, blobLocation)
		if err != nil {
			log.Error("unable to download file: " + err.Error())
		}
	} else if ops == "-l" {
		blob := os.Args[3]
		ListContainer(blob)
	} else {
		log.Error("unsupported operation")
	}

}

func GetAccountInfo() (string, string, string, string) {
	accountKey := os.Getenv("AZ_STORAGE_ACCOUNT_KEY")
	azrBlobAccountName := os.Getenv("AZ_STORAGE_ACCOUNT_NAME")
	azrBlobContainer := os.Getenv("AZ_STORAGE_CONTAINER")
	azrPrimaryBlobServiceEndpoint := fmt.Sprintf("https://%s.blob.core.windows.net/", azrBlobAccountName)

	return accountKey, azrBlobAccountName, azrPrimaryBlobServiceEndpoint, azrBlobContainer
}

func ListContainer(dir string) error {
	dir = strings.TrimSuffix(dir, "/")

	u, _ := url.Parse(fmt.Sprint(endPoint))
	log.Info("endpoint:", u)

	surl := azblob.NewServiceURL(*u, azblob.NewPipeline(credentials, azblob.PipelineOptions{}))
	curl := surl.NewContainerURL(container)
	ctx := context.Background()

	log.Info("listing blob: " + dir)
	for marker := (azblob.Marker{}); marker.NotDone(); {
		list, _ := curl.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{
			Prefix:     dir,
			MaxResults: 5000,
			Details:    azblob.BlobListingDetails{Metadata: true},
		})

		marker = list.NextMarker
		for _, item := range list.Segment.BlobItems {
			dirs := strings.Split(item.Name, "/")
			r_blob := strings.Join(dirs[0:len(dirs)-1], "/")
			if dir == r_blob {
				var btype string = "d"
				if item.Metadata["hdi_isfolder"] != "true" {
					btype = "f"
				}
				fmt.Println(btype, " ", item.Properties.CreationTime, " ", ByteCountDecimal(*item.Properties.ContentLength), " ", item.Name)
			}
		}
	}

	return nil
}

func DowloadBlob(fileName string, outpath string) error {

	var of string
	if outpath == "" {
		of = fmt.Sprint("./", filepath.Base(fileName))
	} else {
		of = fmt.Sprint(outpath, "/", filepath.Base(fileName))
	}

	if FileExist(of) {
		os.Remove(of)
	}

	o_file, _ := os.Create(of)

	u, _ := url.Parse(fmt.Sprint(endPoint, container, "/", fileName))
	log.Info("endpoint:", u)

	surl := azblob.NewBlobURL(*u, azblob.NewPipeline(credentials, azblob.PipelineOptions{}))

	ctx := context.Background()

	err := azblob.DownloadBlobToFile(ctx, surl, 0, azblob.CountToEnd, o_file, azblob.DownloadFromBlobOptions{})

	if err != nil {
		log.Error("file download failed: " + fileName)
		return err
	}

	log.Info("file downloaded")

	return nil
}

func UploadFile(localFile string, remotePath string) error {

	if !FileExist(localFile) {
		log.Error(localFile + " does not exist")
		panic(new(error))
	}
	file := filepath.Base(localFile)
	u, _ := url.Parse(fmt.Sprint(endPoint, container, "/", remotePath, "/", file))
	blobUrl := azblob.NewBlockBlobURL(*u, azblob.NewPipeline(credentials, azblob.PipelineOptions{}))

	log.Info("endpoint:", u)
	ctx := context.Background()

	l_file, ferr := os.Open(localFile)
	if ferr != nil {
		log.Error("unable to read local file: ", localFile)
		panic(ferr)
	}
	_, uerr := azblob.UploadFileToBlockBlob(ctx, l_file, blobUrl, azblob.UploadToBlockBlobOptions{
		BlockSize:   4 * 1024 * 1024,
		Parallelism: 16})

	if uerr != nil {
		log.Error("file upload to azure blob failed", uerr)
		panic(uerr)
	}

	log.Info("file upload completed: " + u.Path)

	return nil
}

func FileExist(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}

	return !info.IsDir()
}

func _Usage() {
	fmt.Println("usage[ " + os.Args[0] + " <contaner> <path> [-c [local dirctory](download), -u <localfile> <bloblocation>(upload) -l (list)]")
}

func ByteCountDecimal(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "kMGTPE"[exp])
}
