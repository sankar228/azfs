package main

import (
	"container/list"
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
	contnr      string
	credentials *azblob.SharedKeyCredential
)

func main() {

	if len(os.Args) < 3 {
		_Usage()
		return
	}
	ops := os.Args[1]
	var outpath string

	log.SetReportCaller(true)
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true,
		CallerPrettyfier: func(frame *runtime.Frame) (function string, file string) {
			fileName := path.Base(frame.File) + ":" + strconv.Itoa(frame.Line)
			//return frame.Function, fileName
			return frame.Func.Name(), fileName
		}})
	log.Info("azfs commands")

	accountKey, accountName, endPoint = GetAccountInfo()
	log.Info("accountName: " + accountName + " endPoint: " + endPoint)

	err := *new(error)
	credentials, err = azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Error("login failed accountname: " + accountName)
	}

	///landing/ran/ericsson/5g/zipped/LBL5ENM1/LBL5ENM1_20210406.zip

	if ops == "--help" || ops == "-h" {
		_Usage()
	} else if ops == "-c" || ops == "--copy" {
		blob := os.Args[2]
		if len(os.Args) == 4 {
			outpath = os.Args[3]
		} else {
			outpath = ""
		}
		err := DowloadBlob(blob, outpath)
		if err != nil {
			log.Error("unable to download file: " + err.Error())
		}
	} else if ops == "-u" || ops == "--upload" {
		if len(os.Args) != 4 {
			_Usage()
		}
		localFie := os.Args[2]
		blobLocation := os.Args[3]
		err := UploadFile(localFie, blobLocation, false)
		if err != nil {
			log.Error("unable to download file: " + err.Error())
		}
	} else if ops == "-l" || ops == "--list" {
		blob := os.Args[2]
		ListContainer(blob, nil)
	} else if ops == "-md" || ops == "--makedir" {
		blob := os.Args[2]
		b_dir := os.Args[3]

		CreateDir(blob, b_dir)
	} else if ops == "-d" || ops == "--delete" {
		blob := os.Args[2]

		DeleteBlob(blob, false)
	} else {
		log.Error("unsupported operation")
		_Usage()
	}

}

func GetAccountInfo() (string, string, string) {
	l_accountKey := os.Getenv("AZ_STORAGE_ACCOUNT_KEY")
	l_accountName := os.Getenv("AZ_STORAGE_ACCOUNT_NAME")
	contnr = os.Getenv("AZ_STORAGE_CONTAINER")

	if l_accountKey == "" {
		log.Error("AZ_STORAGE_ACCOUNT_KEY environment variable is missing, aborting operation")
		panic(new(error))
	} else if l_accountName == "" {
		log.Error("AZ_STORAGE_ACCOUNT_NAME environment variable is missing, aborting operation")
		panic(new(error))
	} else if contnr == "" {
		log.Error("AZ_STORAGE_CONTAINER environment variable is missing, aborting operation")
		panic(new(error))
	}
	azrPrimaryBlobServiceEndpoint := fmt.Sprintf("https://%s.blob.core.windows.net/", l_accountName)

	return l_accountKey, l_accountName, azrPrimaryBlobServiceEndpoint
}

func ListDirectory(dir string) (list.List, error) {

	result := list.New()

	err := ListContainer(dir, result)

	return *result, err
}
func ListContainer(dir string, returnResult *list.List) error {
	dir = strings.TrimSuffix(dir, "/")
	fileRe := "*"
	if strings.HasSuffix(dir, "*") {
		dir, fileRe = filepath.Split(dir)
	}
	u, _ := url.Parse(fmt.Sprint(endPoint))
	log.Info("endpoint: ", u)

	surl := azblob.NewServiceURL(*u, azblob.NewPipeline(credentials, azblob.PipelineOptions{}))
	curl := surl.NewContainerURL(contnr)
	ctx := context.Background()

	preFix := dir + "/"
	log.Info("listing blob: " + dir + " , prefix: " + preFix)
	for marker := (azblob.Marker{}); marker.NotDone(); {
		list, _ := curl.ListBlobsHierarchySegment(ctx, marker, "/", azblob.ListBlobsSegmentOptions{
			Prefix: preFix,
			Details: azblob.BlobListingDetails{
				Metadata: true,
			},
		})

		marker = list.NextMarker

		if len(list.Segment.BlobPrefixes) != 0 {
			for _, item := range list.Segment.BlobPrefixes {
				fmt.Println("D	", item.Name)
			}
		}
		if len(list.Segment.BlobItems) != 0 {
			for _, item := range list.Segment.BlobItems {
				_, f := path.Split(item.Name)
				if matches, _ := filepath.Match(fileRe, f); matches {
					fmt.Println("F	", ByteCountDecimal(*item.Properties.ContentLength), " ", item.Name)
				}
			}
		}
	}

	/*
			for marker := (azblob.Marker{}); marker.NotDone(); {
				list, _ := curl.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{
					Prefix: preFix,
					Details: azblob.BlobListingDetails{
						Metadata: true,
					},
				})

		 		marker = list.NextMarker

				if len(list.Segment.BlobItems) != 0 {
					for _, b := range list.Segment.BlobItems {
						file := strings.Split(b.Name, "/")
						r_blob := strings.Join(file[0:len(file)-1], "/")

						if returnResult != nil {
							returnResult.PushBack(b.Name)
						}

						if dir == r_blob {
							var btype string = "D"
							if b.Metadata["hdi_isfolder"] != "true" {
								btype = "F"
							}
							fmt.Println(btype, " ", b.Properties.LastModified.Format(time.RFC1123), " ", ByteCountDecimal(*b.Properties.ContentLength), " ", b.Name)

						}
					}
				} else {
					log.Error("empty blob: ", dir)
				}
			}*/

	return nil
}

func DowloadBlob(fileName string, outpath string) error {

	var of string
	cwd, _ := os.Getwd()

	fmt.Println("working dir: " + cwd)
	if outpath == "" {
		of = fmt.Sprint(cwd, filepath.Base(fileName))
	} else {
		of = fmt.Sprint(outpath, "/", filepath.Base(fileName))
	}

	if FileExist(of) {
		os.Remove(of)
	}

	o_file, _ := os.Create(of)

	u, _ := url.Parse(fmt.Sprint(endPoint, contnr, "/", fileName))
	log.Info("endpoint: ", u)

	surl := azblob.NewBlobURL(*u, azblob.NewPipeline(credentials, azblob.PipelineOptions{}))

	ctx := context.Background()

	err := azblob.DownloadBlobToFile(ctx, surl, 0, azblob.CountToEnd, o_file, azblob.DownloadFromBlobOptions{})

	if err != nil {
		log.Error("file download failed: " + fileName)
		return err
	}

	log.Info("file downloaded: " + of)

	return nil
}

func UploadFile(localFile string, remotePath string, mutelog bool) error {

	if !FileExist(localFile) {
		log.Error(localFile + " does not exist")
		panic(new(error))
	}
	file := filepath.Base(localFile)
	u, _ := url.Parse(fmt.Sprint(endPoint, contnr, "/", remotePath, "/", file))
	blobUrl := azblob.NewBlockBlobURL(*u, azblob.NewPipeline(credentials, azblob.PipelineOptions{}))

	if !mutelog {
		log.Info("endpoint:", u)
	}
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

	if !mutelog {
		log.Info("file upload completed: " + u.Path)
	}

	return nil
}

func CreateDir(blob string, directory string) error {
	tempLocalFile, err := os.CreateTemp("./", ".temp_temp")
	dir := blob + "/" + directory
	if err == nil {
		UploadFile(tempLocalFile.Name(), dir, true)
		DeleteBlob(dir+"/"+tempLocalFile.Name(), true)
		os.Remove(tempLocalFile.Name())
	} else {
		return err
	}

	log.Info("Directory created: " + dir)

	return nil
}

func DeleteBlob(blob string, mutelog bool) {
	var blobPath string

	if strings.HasPrefix(blob, "/") {
		blobPath = contnr + blob
	} else {
		blobPath = contnr + "/" + blob
	}
	u, _ := url.Parse(fmt.Sprint(endPoint, blobPath))
	blobUrl := azblob.NewBlockBlobURL(*u, azblob.NewPipeline(credentials, azblob.PipelineOptions{}))

	if !mutelog {
		log.Info("endpoint: ", u)
	}
	ctx := context.Background()

	entries, err := ListDirectory(blob)
	if err != nil {
		panic(err)
	}
	var sureDelete string

	if mutelog {
		sureDelete = "Y"
	} else {
		if entries.Len() > 1 {
			fmt.Println("directory has ", entries.Len(), " files still want delete directory: "+blobPath+" Y/N ?")
		} else {
			fmt.Println("delete blob entry: " + blobPath + " Y/N ?")
		}
		fmt.Scanln(&sureDelete)
	}
	if strings.EqualFold(sureDelete, "Y") {
		if entries.Len() > 1 {
			DeleteMultiBlob(contnr, &entries, false)
		}
		deleteResp, err := blobUrl.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		if err == nil {
			if !mutelog {
				log.Info("Blob deleted, response: {}, {}"+deleteResp.ErrorCode(), deleteResp.Status())
			}
		} else {
			fmt.Println("error deleting the blob entry: ", err)
		}

	}
}

func DeleteMultiBlob(container string, entries *list.List, mutelog bool) {
	ctx := context.Background()

	for file := entries.Front(); file != nil; file = file.Next() {
		blobFile := container + "/" + file.Value.(string)
		fmt.Println("deleting file: ", blobFile)
		u, _ := url.Parse(fmt.Sprint(endPoint, blobFile))
		blobFileUrl := azblob.NewBlockBlobURL(*u, azblob.NewPipeline(credentials, azblob.PipelineOptions{}))

		deleteResp, err := blobFileUrl.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		if err == nil {
			if !mutelog {
				log.Info("Blob deleted, response: {}, {}"+deleteResp.ErrorCode(), deleteResp.Status())
			}
		} else {
			fmt.Println("error deleting the blob entry: ", err)
		}
	}
}

func FileExist(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}

	return !info.IsDir()
}

func _Usage() {
	fmt.Println("usage[ " + os.Args[0] + "  <options> \n" +
		"-h | --help : Help !! \n" +
		"-c | --copy : Copy/Dowload file from the blob storage eg: -c <blob path> [local dirctory] default to current dir \n" +
		"-u | --upload : Upload a file to blob storage eg: -u <localfile> <bloblocation> \n" +
		"-l | --list : List directories or files a given blob/container eg: -l <blob path> \n" +
		"-d | --delete : Delete a file/blob from the blob storage -d <blob path> \n" +
		"-md | --makedir : Create folder/dir in a given blob path eg: -md <blob path> <dir Name>\n")
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
