package main

import ("log"
		"flag"
		"context"
		"fmt"
		"time"
		"github.com/aws/aws-sdk-go-v2/aws"
		"github.com/aws/aws-sdk-go-v2/config"
		"github.com/aws/aws-sdk-go-v2/service/s3"
		s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	)

func getPrefixes(client *s3.Client, bucketPtr *string, prefixPtr *string) ([]*string) {
	// Get the first page of results for ListObjectsV2 for a bucket
	var res []*string;
	var continuationToken *string;

	for  {
		output, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket: bucketPtr,
			Prefix: prefixPtr,
			Delimiter: aws.String("/"),
			ContinuationToken: continuationToken,			
		})
		if err != nil {
			log.Fatal(err)
		}

		for _, object := range output.CommonPrefixes {
			res = append(res, object.Prefix)
		}

		if(*output.IsTruncated) {
			continuationToken = output.NextContinuationToken
		} else {
			break
		}	
	}

	return res;
}

// object -- a key in S3
// deleted object -- an object which was deleted but preserved by revisions
// current object -- is not deleted
// version -- a revision of an object (not including delete markers)
// current version -- the visible version of an object
// PrevVersion -- a version which is not visible
// old -- was created > 7 days ago and is not a visible object
// multi version -- has > 1 version of the object
// duplicate version -- a versio of an object which has the same checksum as another version of the same object

// age bands:   1 - < 7 days, 2 - < 30 days, 3 - < 180 days 4 - older




type OneKeyStats struct {
	key *string
	IsDeleted bool
	NoCurrent bool

	NumDeleteMarkers int64
	NumHistDeleteMarkers int64
	NumVersions int64

	IsOld bool

	Size int64
	DuplicateSize int64
	ETags map[*string]int
}

func (ks *OneKeyStats) handleDeleteMarker(dm *s3types.DeleteMarkerEntry, isOld bool) {
	if *dm.IsLatest {
		ks.IsDeleted = true
		ks.NoCurrent = false
		ks.IsOld = isOld
	} else {
		ks.NumHistDeleteMarkers ++;
	}

	ks.NumDeleteMarkers ++;	
}

func (ks *OneKeyStats) handleObjectVersion(ov *s3types.ObjectVersion, isOld bool) {
	if _, ok := ks.ETags[ov.ETag] ; ok {
		ks.ETags[ov.ETag] ++
		ks.DuplicateSize += *ov.Size
	} else {
		ks.ETags[ov.ETag] = 1
	}

	ks.IsOld = isOld

	if *ov.IsLatest {
		ks.IsDeleted = false
		ks.NoCurrent = false
	}
	ks.NumVersions ++
	ks.Size += *ov.Size
}

type KSMMapType map[string]*OneKeyStats;

type KeyStatMap struct {
	stats KSMMapType
}

func (ksm *KeyStatMap) getKeyStats(key *string) (*OneKeyStats) {
	if ksm.stats == nil {
		ksm.stats = KSMMapType{}
	}

	s, ok := ksm.stats[*key]

	if ! ok {
		ns := OneKeyStats{
			key: key,
			ETags: map[*string]int{},
			NoCurrent: true,
		}
		ksm.stats[*key] = &ns
		return &ns
	}

	return s
}

func (ksm *KeyStatMap) handleDeleteMarker(dm *s3types.DeleteMarkerEntry, isOld bool) {
	ksm.getKeyStats(dm.Key).handleDeleteMarker(dm, isOld)
}

func (ksm *KeyStatMap) handleObjectVersion(ov *s3types.ObjectVersion, isOld bool) {
	ksm.getKeyStats(ov.Key).handleObjectVersion(ov, isOld)
}



type AllKeyStats struct {
	// counters
	NumObjects int64
	NumCurrentObjects int64
	NumDeletedObjects int64
	NumNoCurrentObjects int64

	NumVersions int64
	NumCurrentVersions int64
	NumDuplicateVersions int64
	NumDeleteMarkers int64
	NumCurrentDeleteMarkers int64
	NumHistDeleteMarkers int64

	NumMultiVerFiles int64

	NumOldVersions int64
	NumOldDeleteMarkers int64
	NumOldCurrentObjects int64
	NumOldDeletedObjects int64


	CurrentObjectBytes int64
	DeletedObjectBytes int64
	CurrentVersionBytes int64
	DeletedVersionBytes int64
	DuplicateObjectBytes int64
	OldCurrentObjectBytes int64
	OldDeletedObjectBytes int64
	OldVersionBytes int64

	BiggestMultiverFile *string
	BiggestMultiverFileVersions int64
}


func (aks *AllKeyStats) includeDeleteMarker (dm *s3types.DeleteMarkerEntry, isOld bool) {
	aks.NumDeleteMarkers ++
	if *dm.IsLatest {
		aks.NumCurrentDeleteMarkers ++
	} else {
		aks.NumHistDeleteMarkers ++
	}
	if isOld {
		aks.NumOldDeleteMarkers ++
	}
}

func (aks *AllKeyStats) includeObjectVersion (ov *s3types.ObjectVersion, isOld bool) {
	aks.NumVersions ++
	if *ov.IsLatest {
		aks.NumCurrentVersions ++
		aks.CurrentVersionBytes += *ov.Size
	} else {
		aks.DeletedVersionBytes += *ov.Size
	}
	if isOld {
		aks.NumOldVersions ++
		aks.OldVersionBytes += *ov.Size
	}
}

func (aks *AllKeyStats) includeOneKeyStats (ks *OneKeyStats) {
	aks.NumDuplicateVersions += (ks.NumVersions - int64(len(ks.ETags)))
	if ks.IsDeleted {
		aks.NumDeletedObjects ++
		aks.DeletedObjectBytes += ks.Size
	} else {
		aks.NumCurrentObjects ++
		aks.CurrentObjectBytes += ks.Size
	}
	aks.NumObjects ++
	aks.DuplicateObjectBytes += ks.DuplicateSize

	if ks.NumVersions > 1 || ks.NumDeleteMarkers > 1 {
		aks.NumMultiVerFiles ++
	}

	if ks.NoCurrent {
		aks.NumNoCurrentObjects ++
	}

	if ks.NumVersions > aks.BiggestMultiverFileVersions {
		aks.BiggestMultiverFile = ks.key
		aks.BiggestMultiverFileVersions = ks.NumVersions
	}

	if ks.IsOld  {
		if ks.IsDeleted {
			aks.NumOldDeletedObjects ++
			aks.OldDeletedObjectBytes += ks.Size
		} else {
			aks.NumOldCurrentObjects ++;
			aks.OldCurrentObjectBytes += ks.Size
		}
	} 
}

func (aks *AllKeyStats) includeKeyStatMap (ksm *KeyStatMap) {
	for _, ks := range ksm.stats {
		aks.includeOneKeyStats (ks)
	}
}

func (aks *AllKeyStats) merge(nks *AllKeyStats) {	
	aks.NumObjects += nks.NumObjects
	aks.NumCurrentObjects += nks.NumCurrentObjects
	aks.NumDeletedObjects += nks.NumDeletedObjects
	aks.NumNoCurrentObjects += nks.NumNoCurrentObjects

	aks.NumVersions += nks.NumVersions
	aks.NumCurrentVersions += nks.NumCurrentVersions
	aks.NumDuplicateVersions += nks.NumDuplicateVersions
	aks.NumDeleteMarkers += nks.NumDeleteMarkers
	aks.NumCurrentDeleteMarkers += nks.NumCurrentDeleteMarkers
	aks.NumHistDeleteMarkers += nks.NumHistDeleteMarkers

	aks.NumMultiVerFiles += nks.NumMultiVerFiles

	aks.NumOldVersions += nks.NumOldVersions
	aks.NumOldDeleteMarkers += nks.NumOldDeleteMarkers
	aks.NumOldCurrentObjects += nks.NumOldCurrentObjects	
	aks.NumOldDeletedObjects += nks.NumOldDeletedObjects

	aks.CurrentObjectBytes += nks.CurrentObjectBytes
	aks.DeletedObjectBytes += nks.DeletedObjectBytes
	aks.CurrentVersionBytes += nks.CurrentVersionBytes
	aks.DeletedVersionBytes += nks.DeletedVersionBytes
	aks.DuplicateObjectBytes += nks.DuplicateObjectBytes
	aks.OldCurrentObjectBytes += nks.OldCurrentObjectBytes
	aks.OldDeletedObjectBytes += nks.OldDeletedObjectBytes
	aks.OldVersionBytes += nks.OldVersionBytes

	if nks.BiggestMultiverFileVersions > aks.BiggestMultiverFileVersions {
		aks.BiggestMultiverFileVersions = nks.BiggestMultiverFileVersions
		aks.BiggestMultiverFile = nks.BiggestMultiverFile
	}
}

func (aks AllKeyStats) ToString() string {
	res := fmt.Sprintf("NumObjects %d\nNumCurrentObjects %d\nNumDeletedObjects %d\nNumNoCurrentObjects %d\n",
		aks.NumObjects,
		aks.NumCurrentObjects,
		aks.NumDeletedObjects,
		aks.NumNoCurrentObjects,
	)

	res += fmt.Sprintf("\nNumVersions %d\nNumCurrentVersions %d\nNumDuplicateVersions %d\nNumDeleteMarkers %d\nNumCurrentDeleteMarkers %d\nNumHistDeleteMarkers %d\n\n",
		aks.NumVersions,
		aks.NumCurrentVersions,
		aks.NumDuplicateVersions,
		aks.NumDeleteMarkers,
		aks.NumCurrentDeleteMarkers,
		aks.NumHistDeleteMarkers,
	)
	res += fmt.Sprintf("NumOldVersions %d\nNumOldDeleteMarkers %d\nNumOldCurrentObjects %d\nNumOldDeletedObjects %d\n\n",
		aks.NumOldVersions,
		aks.NumOldDeleteMarkers,
		aks.NumOldCurrentObjects,
		aks.NumOldDeletedObjects,
	)

	res += fmt.Sprintf("NumMultiVerFiles %d\nBiggersMultiverFile %s\nBiggestFileVersions %d\n",
		aks.NumMultiVerFiles,
		*aks.BiggestMultiverFile,
		aks.BiggestMultiverFileVersions,
	)

	res += fmt.Sprintf("CurrentObjectBytes %d\nDeletedObjectBytes %d\nCurrentVersionBytes %d\nDeletedVersionBytes %d\nDuplicateObjectBytes %d\n\n", 
		aks.CurrentObjectBytes, 
		aks.DeletedObjectBytes, 
		aks.CurrentVersionBytes, 
		aks.DeletedVersionBytes, 
		aks.DuplicateObjectBytes,
	)

	res += fmt.Sprintf("OldCurrentObjectBytes %d\nOldDeletedObjectBytes %d\nOldVersoinBytes %d\n\n", 
		aks.OldCurrentObjectBytes,
		aks.OldDeletedObjectBytes,
		aks.OldVersionBytes,
	)
	return res
}

type taskStatus struct {		
	starting bool
	stopping bool
	objects int
	markers int
}



func getObjectVersionStats(config *aws.Config, bucketPtr *string, prefixPtr *string, timeNow time.Time, notificationChan chan <- taskStatus, oldAge *int) (*AllKeyStats){
	client := s3.NewFromConfig(*config)
	var keyMarker *string
	var versionIdMarker *string

	var ksm KeyStatMap

	var aks AllKeyStats
	var oProcessed int64
	var oOutput int64
	var dmProcessed int64

	notificationChan <- taskStatus{
		starting: true,
	}

	for {
		output, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
			Bucket: bucketPtr,
			Prefix: prefixPtr,
			KeyMarker: keyMarker,
			VersionIdMarker: versionIdMarker,
		})
		if err != nil {
			log.Fatal(err)
		}
		
		for _, dm := range output.DeleteMarkers {
			dmProcessed ++;
			isOld := int(timeNow.Sub(*dm.LastModified).Hours() / 24) > *oldAge
			aks.includeDeleteMarker(&dm, isOld)
			ksm.handleDeleteMarker (&dm, isOld)
		}

		for _, ov := range output.Versions {
			oProcessed ++
			isOld := int(timeNow.Sub(*ov.LastModified).Hours() / 24) > *oldAge
			aks.includeObjectVersion(&ov, isOld)
			ksm.handleObjectVersion (&ov, isOld)
		}

		if oProcessed - oOutput > 10000 {
			//log.Printf("Prefix %s handled %d objects and %d delete markers.", *prefixPtr, oProcessed, dmProcessed)
			oOutput = oProcessed;
		}

		notificationChan <- taskStatus{
			objects: len(output.Versions),
			markers: len(output.DeleteMarkers),
		}
	

		if(*output.IsTruncated) {
			keyMarker = output.NextKeyMarker
			versionIdMarker = output.NextVersionIdMarker
		} else {
			if oProcessed > 0 {
				//log.Printf("Prefix %s handled %d objects and %d delete markers.", *prefixPtr, oProcessed, dmProcessed)
			}
			break
		}		
	}

	notificationChan <- taskStatus{
		stopping: true,
	}

	aks.includeKeyStatMap(&ksm)

	return &aks
}

func main () {
	log.Println("Diskscan")

	bucketPtr := flag.String("bucket", "", "Bucket Name")
	prefixPtr := flag.String("prefix", "", "Bucket Prefix")

	numThreads := flag.Int("threads", 100, "Number of threads")
	oldAge := flag.Int("oldage", 7, "Files older than this many days considered old")
	pfMult := flag.Bool("pfmult", false, "Multiply prefixes by [0-9a-f]")

	flag.Parse()

	log.Println("Bucket: ", *bucketPtr)
	log.Println("Prefix: ", *prefixPtr)
	log.Println("Threads: ", *numThreads)
	log.Println("Old Age: ", *oldAge)
	log.Println("Pfmult: ", *pfMult)

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	timeNow := time.Now()

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(cfg)

	// top level control
	var aks AllKeyStats
	results := make(chan *AllKeyStats, 1000)
	var resultsToCome int

	// tracker
	notificationChan := make(chan taskStatus, 1000)

	go func() {
		numRunning := 0
		numCompleted := 0
		objectsScanned := int64(0)
		markersScanned := int64(0)
		
		lastOutTime := timeNow
		startTime := timeNow
		lastOutObjects := int64(0)

		for {
			next := <- notificationChan
			if next.starting {
				numRunning ++
			}
			if next.stopping {
				numRunning --
				numCompleted ++
			}
			objectsScanned += int64(next.objects)
			markersScanned += int64(next.markers)

			tn := time.Now()
			interval := tn.Sub(lastOutTime).Seconds()
			runtime := tn.Sub(startTime).Seconds()

			if interval > 10 {
				rate := float64(objectsScanned - lastOutObjects) / interval
				allrate := float64(objectsScanned ) / runtime
				log.Printf("TRACKER:  Threads %d, Objects %d, Markers %d, objects/second %d, Overall o/s %d, Prefixes %d", 
					numRunning, objectsScanned, markersScanned, int64(rate), int64(allrate), numCompleted)
				lastOutObjects = objectsScanned;
				lastOutTime = tn
			}
		}
	}()

	// worker threads

	tasks := make(chan *string, 100000)

	for threadNum:=0; threadNum < *numThreads ; threadNum ++ {
		go func(myThread int) {
			for {
				nextPrefix := <-tasks
				if nextPrefix == nil {
					//log.Printf("Thread %d exiting now", myThread)
					break
				} else {
					//log.Printf("Thread %d working on prefix %s", myThread, *nextPrefix)

					results <- getObjectVersionStats(&cfg, bucketPtr, nextPrefix, timeNow, notificationChan, oldAge)
					//log.Printf("Thread %d done with prefix %s", myThread, *nextPrefix)
				}
			}
		} (threadNum)
	}

	// top level task creation

	for _,st := range getPrefixes(client, bucketPtr, prefixPtr) {
		log.Printf("Got Prefix=%s", *st)
		if *pfMult {
			for n:=0; n < 16; n ++ {
				pf := fmt.Sprintf("%s%x", *st, n)
				tasks <- &pf
				resultsToCome ++
			}
		} else {
			tasks <- st
			resultsToCome ++
		}		
	}

	for threadNum := 0; threadNum < *numThreads ; threadNum ++ {
		tasks <- nil    // one terminator for each thread will terminate them all
						// We don't really care if they stop or not but this is nicer.
	}

	for ; resultsToCome > 0; resultsToCome -- {
		aks.merge (<- results)		
	}

	log.Print("Final results:\n", aks.ToString())
}