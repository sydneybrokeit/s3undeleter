package main

import (
	"flag"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
)

var region = flag.String("region", "us-east-1", "region bucket is in")
var threads = flag.Int("threads", 8, "threads to use for running the API calls")
var debug = flag.Bool("debug", false, "enable debugging to prev")
var bucket = flag.String("bucket", "bucket.bucket.bucket.bucket", "bucket to undelete from")
var requestThreads = flag.Int("request_threads", 16, "threads to use for making the delete requests")
var prefix = flag.String("prefix", "", "prefix to use")

var s3client *s3.S3
var pageChan = make(chan *s3.ListObjectVersionsOutput, 64)
var versionsChan = make(chan s3.ObjectIdentifier)
var deletesDoneChan = make(chan bool)
var doneChan = make(chan bool)
var endChan = make(chan bool)

func main() {
	flag.Parse()
	if *debug {
		log.SetLevel(log.DebugLevel)
	}
	s3config := &aws.Config{
		Region: region,
	}
	s3session, err := session.NewSession(s3config)
	if err != nil {
		log.Fatalf("could not create s3 session %v", err)
	}
	s3client = s3.New(s3session)
	for i := 0; i < *threads; i++ {
		log.Debugf("starting thread %d of %d", i + 1, *threads)
		go GetDeleteMarkers(pageChan, versionsChan)
	}
	for i := 0; i < *requestThreads; i++ {
		log.Debugf("starting delete thread %d of %d", i+1, *requestThreads)
		go SendDeleteRequestsLoop(versionsChan, deletesDoneChan, endChan)
	}
	var request = &s3.ListObjectVersionsInput{}
	if *prefix != "" {
		request = &s3.ListObjectVersionsInput{
			Bucket: bucket,
			Prefix: prefix,
		}
	} else {
		request = &s3.ListObjectVersionsInput{
			Bucket: bucket,
		}
	}

	err = s3client.ListObjectVersionsPages(request, SendPagesToChannel)
	if err != nil {
		log.Fatalf("error getting pages %s", err)
	}
	for i := 0; i < *requestThreads; i++ {
		<- endChan
		log.Info("received end from thread %d of %d", i+1, *requestThreads)
	}

}

func SendDeleteRequestsLoop(versions chan s3.ObjectIdentifier, done chan bool, end chan bool) {
	deletes := 0
	var objects []*s3.ObjectIdentifier
	for {
		select {
		case version := <- versions:
			deletes += 1
			objects = append(objects, &version)
			if deletes >= 1000 {
				err := DeleteItems(objects)
				if err != nil {
					log.Warnf("failed to delete objects %s", err)
				}
				deletes = 0
				objects = []*s3.ObjectIdentifier{}
			}
		case <- done:
			err :=DeleteItems(objects)
			if err != nil {
				log.Warnf("failed to delete objects %s", err)
			}
			end <- true
			return
		}
	}
}

func DeleteItems(objects []*s3.ObjectIdentifier) (err error) {
	deleteObjects := &s3.DeleteObjectsInput{
		Bucket: bucket,
		Delete: &s3.Delete{
			Objects: objects,
		},
	}
	_, err = s3client.DeleteObjects(deleteObjects)
	return
}

var pageCount = 0
// send the pages from the ListObjectVersionPages response to a channel for threaded processing
func SendPagesToChannel(page *s3.ListObjectVersionsOutput, lastPage bool) bool {
	pageCount += 1
	log.Debug("sending page %d", pageCount)
	pageChan <- page
	// if we're processing the last page, send a done message for each thread doing the processing
	if lastPage {
		for i := 0; i < *threads; i++ {
			doneChan <- true
		}
	}
	return ! lastPage
}

// process delete markers to
func GetDeleteMarkers(pages chan *s3.ListObjectVersionsOutput, versions chan s3.ObjectIdentifier) {
	for {
		select {
		case page := <-pages:
			for _, deleteMarker := range page.DeleteMarkers {
				object := s3.ObjectIdentifier{
					Key:       deleteMarker.Key,
					VersionId: deleteMarker.VersionId,
				}
				versions <- object
			}
			break
		case <- doneChan:
			log.Warnf("done")
			return
		}
	}
}


