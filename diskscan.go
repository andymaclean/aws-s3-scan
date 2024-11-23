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

	age int

	Size int64
	DuplicateSize int64
	ETags map[*string]int
}

func (ks *OneKeyStats) handleDeleteMarker(dm *s3types.DeleteMarkerEntry, age int) {
	if *dm.IsLatest {
		ks.IsDeleted = true
		ks.NoCurrent = false
		ks.age = age
	} else {
		ks.NumHistDeleteMarkers ++;
	}

	ks.NumDeleteMarkers ++;	
}

func (ks *OneKeyStats) handleObjectVersion(ov *s3types.ObjectVersion, age int) {
	if _, ok := ks.ETags[ov.ETag] ; ok {
		ks.ETags[ov.ETag] ++
		ks.DuplicateSize += *ov.Size
		ks.age = age
	} else {
		ks.ETags[ov.ETag] = 1
	}

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

func (ksm *KeyStatMap) handleDeleteMarker(dm *s3types.DeleteMarkerEntry, age int) {
	ksm.getKeyStats(dm.Key).handleDeleteMarker(dm, age)
}

func (ksm *KeyStatMap) handleObjectVersion(ov *s3types.ObjectVersion, age int) {
	ksm.getKeyStats(ov.Key).handleObjectVersion(ov, age)
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


func (aks *AllKeyStats) includeDeleteMarker (dm *s3types.DeleteMarkerEntry, age int) {
	aks.NumDeleteMarkers ++
	if *dm.IsLatest {
		aks.NumCurrentDeleteMarkers ++
	} else {
		aks.NumHistDeleteMarkers ++
	}
	if age > 7 {
		aks.NumOldDeleteMarkers ++
	}
}

func (aks *AllKeyStats) includeObjectVersion (ov *s3types.ObjectVersion, age int) {
	aks.NumVersions ++
	if *ov.IsLatest {
		aks.NumCurrentVersions ++
		aks.CurrentVersionBytes += *ov.Size
	} else {
		aks.DeletedVersionBytes += *ov.Size
	}
	if age > 7 {
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

	if ks.age > 7  {
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


func getObjectVersionStats(config *aws.Config, bucketPtr *string, prefixPtr *string, timeNow time.Time) (*AllKeyStats){
	client := s3.NewFromConfig(*config)
	var keyMarker *string
	var versionIdMarker *string

	var ksm KeyStatMap

	var aks AllKeyStats
	var oProcessed int64
	var oOutput int64
	var dmProcessed int64

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
			age := int(timeNow.Sub(*dm.LastModified).Hours() / 24)
			aks.includeDeleteMarker(&dm, age)
			ksm.handleDeleteMarker (&dm, age)
		}

		for _, ov := range output.Versions {
			oProcessed ++
			age := int(timeNow.Sub(*ov.LastModified).Hours() / 24)
			aks.includeObjectVersion(&ov, age)
			ksm.handleObjectVersion (&ov, age)
		}

		if oProcessed - oOutput > 10000 {
			log.Printf("Prefix %s handled %d objects and %d delete markers.", *prefixPtr, oProcessed, dmProcessed)
			oOutput = oProcessed;
		}

		if(*output.IsTruncated) {
			keyMarker = output.NextKeyMarker
			versionIdMarker = output.NextVersionIdMarker
		} else {
			break
		}		

		if oProcessed > 25000 {
			break
		}
	}

	aks.includeKeyStatMap(&ksm)

	return &aks
}

func main () {
	log.Println("Diskscan")

	bucketPtr := flag.String("bucket", "", "Bucket Name")
	prefixPtr := flag.String("prefix", "", "Bucket Prefix")

	flag.Parse()

	log.Println("Bucket: ", *bucketPtr)
	log.Println("Bucket: ", *prefixPtr)

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	timeNow := time.Now()

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(cfg)
	
	var aks AllKeyStats
	results := make(chan *AllKeyStats, 1000)
	var resultsToCome int

	for _,st := range getPrefixes(client, bucketPtr, prefixPtr) {
		log.Printf("Got Prefix=%s", *st)
		go func () {
			results <- getObjectVersionStats(&cfg, bucketPtr, st, timeNow)
		} ()
		resultsToCome ++
	}

	for ; resultsToCome > 0; resultsToCome -- {
		aks.merge (<- results)		
	}

	log.Print("Final results:\n", aks.ToString())
}