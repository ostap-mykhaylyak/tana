package s3

import (
	"encoding/xml"
	"time"
)

// The XML documents the S3 API exchanges. Only the fields tana fills
// are declared: an SDK ignores what it does not need, and a struct
// that lists everything S3 can return would imply tana implements it.

// ListAllMyBucketsResult answers GET /.
type ListAllMyBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Owner   Owner    `xml:"Owner"`
	Buckets struct {
		Bucket []BucketEntry `xml:"Bucket"`
	} `xml:"Buckets"`
}

// BucketEntry is one bucket in a listing.
type BucketEntry struct {
	Name         string    `xml:"Name"`
	CreationDate time.Time `xml:"CreationDate"`
}

// Owner identifies the caller. tana has one identity per bucket, so
// this is filled with the bucket's own name rather than invented.
type Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

// ListBucketResult answers ListObjectsV2.
type ListBucketResult struct {
	XMLName               xml.Name       `xml:"ListBucketResult"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	MaxKeys               int            `xml:"MaxKeys"`
	KeyCount              int            `xml:"KeyCount"`
	IsTruncated           bool           `xml:"IsTruncated"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	StartAfter            string         `xml:"StartAfter,omitempty"`
	EncodingType          string         `xml:"EncodingType,omitempty"`
	Contents              []ObjectEntry  `xml:"Contents"`
	CommonPrefixes        []CommonPrefix `xml:"CommonPrefixes"`
}

// ObjectEntry is one object in a listing.
type ObjectEntry struct {
	Key          string    `xml:"Key"`
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
	Size         int64     `xml:"Size"`
	StorageClass string    `xml:"StorageClass"`
}

// CommonPrefix is a directory-like grouping produced by a delimiter.
type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

// DeleteRequest is the body of a batch delete.
type DeleteRequest struct {
	XMLName xml.Name           `xml:"Delete"`
	Quiet   bool               `xml:"Quiet"`
	Objects []DeleteRequestKey `xml:"Object"`
}

// DeleteRequestKey is one key in a batch delete.
type DeleteRequestKey struct {
	Key string `xml:"Key"`
}

// DeleteResult answers a batch delete.
type DeleteResult struct {
	XMLName xml.Name        `xml:"DeleteResult"`
	Deleted []DeletedObject `xml:"Deleted"`
	Errors  []DeleteError   `xml:"Error"`
}

// DeletedObject reports one successful deletion.
type DeletedObject struct {
	Key string `xml:"Key"`
}

// DeleteError reports one failed deletion.
type DeleteError struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// CopyObjectResult answers a server-side copy.
type CopyObjectResult struct {
	XMLName      xml.Name  `xml:"CopyObjectResult"`
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
}

// InitiateMultipartUploadResult answers POST ?uploads.
type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

// CompleteMultipartUpload is the body listing the parts to assemble.
type CompleteMultipartUpload struct {
	XMLName xml.Name           `xml:"CompleteMultipartUpload"`
	Parts   []CompletedPartRef `xml:"Part"`
}

// CompletedPartRef is one part a client claims to have uploaded.
type CompletedPartRef struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

// CompleteMultipartUploadResult answers the completion.
type CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

// ListPartsResult answers GET ?uploadId.
type ListPartsResult struct {
	XMLName     xml.Name   `xml:"ListPartsResult"`
	Bucket      string     `xml:"Bucket"`
	Key         string     `xml:"Key"`
	UploadID    string     `xml:"UploadId"`
	MaxParts    int        `xml:"MaxParts"`
	IsTruncated bool       `xml:"IsTruncated"`
	Parts       []PartInfo `xml:"Part"`
}

// PartInfo describes one uploaded part.
type PartInfo struct {
	PartNumber   int       `xml:"PartNumber"`
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
	Size         int64     `xml:"Size"`
}

// ListMultipartUploadsResult answers GET ?uploads.
type ListMultipartUploadsResult struct {
	XMLName     xml.Name     `xml:"ListMultipartUploadsResult"`
	Bucket      string       `xml:"Bucket"`
	IsTruncated bool         `xml:"IsTruncated"`
	Uploads     []UploadInfo `xml:"Upload"`
}

// UploadInfo describes one upload in progress.
type UploadInfo struct {
	Key       string    `xml:"Key"`
	UploadID  string    `xml:"UploadId"`
	Initiated time.Time `xml:"Initiated"`
}
