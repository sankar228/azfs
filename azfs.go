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
	accountKey := "R3lBnSAuXNXc4wtXYUuVgfqo+hB9kbWIM8wwo9aq25XNrWNRj78QlnY6q8+YYZMYUZt6zlYjBj+8hvfnOhZ1vQ=="
	azrBlobAccountName := "qtmdlnedwu2sa0"
	azrPrimaryBlobServiceEndpoint := fmt.Sprintf("https://%s.blob.core.windows.net/", azrBlobAccountName)
	azrBlobContainer := "root"

	return accountKey, azrBlobAccountName, azrPrimaryBlobServiceEndpoint, azrBlobContainer
}

func ListContainer(dir string) error {
	accountKey, accountName, endPoint, container := GetAccountInfo()

	creden, ec := azblob.NewSharedKeyCredential(accountName, accountKey)
	if ec != nil {
		log.Error("login failed accountname: " + accountName)
		return ec
	}

	u, _ := url.Parse(fmt.Sprint(endPoint, container))
	log.Info("endpoint:", u)

	surl := azblob.NewContainerURL(*u, azblob.NewPipeline(creden, azblob.PipelineOptions{}))

	ctx := context.Background()

	log.Info("listing blob: " + dir)
	for marker := (azblob.Marker{}); marker.NotDone(); {
		list, _ := surl.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{})

		marker = list.NextMarker
		for _, item := range list.Segment.BlobItems {
			if strings.Contains(item.Name, dir) {
				fmt.Println(item.Properties.CreationTime, " ", item.Name)
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
