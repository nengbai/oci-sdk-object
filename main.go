/* Copyright (c) 2016, 2018, 2022, Oracle and/or its affiliates.  All rights reserved.
   This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
   Example code for Object Storage Service API
   Author: neng.bai@oracle.com
   Team: Bigdata
*/
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/example/helpers"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/oracle/oci-go-sdk/v65/objectstorage/transfer"
)

// ExampleObjectStorage_UploadFile shows how to create a bucket and upload a file
/*
    Output:
	get namespace
	create bucket
	put object
	delete object
	delete bucket
*/
func ExampleObjectStorage_UploadFile() {
	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(common.CustomProfileConfigProvider(ociConfigFilePath, ociProfileName))
	helpers.FatalIfError(err)

	ctx := context.Background()
	bname := helpers.GetRandomString(8)
	namespace := getNamespace(ctx, client)

	createBucket(ctx, client, namespace, bname)
	defer deleteBucket(ctx, client, namespace, bname)

	contentlen := 1024 * 1000
	filepath, filesize := helpers.WriteTempFileOfSize(int64(contentlen))
	filename := path.Base(filepath)
	defer func() {
		os.Remove(filename)
	}()

	file, e := os.Open(filepath)
	if e != nil {
		helpers.FatalIfError(e)
	}
	defer file.Close()

	e = putObject(ctx, client, namespace, bname, filename, filesize, file, nil)
	helpers.FatalIfError(e)
	defer deleteObject(ctx, client, namespace, bname, filename)
}

func ExampleObjectStorage_UploadManager_UploadFile() {
	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(common.CustomProfileConfigProvider(ociConfigFilePath, ociProfileName))
	helpers.FatalIfError(err)
	// Disable timeout to support big file upload(Once need to specify the os client for Upload Manager)
	client.HTTPClient = &http.Client{}

	ctx := context.Background()
	bname := "bname"
	namespace := getNamespace(ctx, client)

	createBucket(ctx, client, namespace, bname)
	defer deleteBucket(ctx, client, namespace, bname)

	contentlen := 1024 * 1024 * 300 // 300MB
	filepath, _ := helpers.WriteTempFileOfSize(int64(contentlen))
	filename := path.Base(filepath)
	defer os.Remove(filename)

	uploadManager := transfer.NewUploadManager()
	objectName := "sampleFileUploadObj"

	req := transfer.UploadFileRequest{
		UploadRequest: transfer.UploadRequest{
			NamespaceName:                       common.String(namespace),
			BucketName:                          common.String(bname),
			ObjectName:                          common.String(objectName),
			PartSize:                            common.Int64(128 * 1024 * 1024),
			CallBack:                            callBack,
			ObjectStorageClient:                 &client,
			EnableMultipartChecksumVerification: common.Bool(true),
		},
		FilePath: filepath,
	}

	// if you want to overwrite default value, you can do it
	// as: transfer.UploadRequest.AllowMultipartUploads = common.Bool(false) // default is true
	// or: transfer.UploadRequest.AllowParrallelUploads = common.Bool(false) // default is true
	resp, err := uploadManager.UploadFile(ctx, req)

	if err != nil && resp.IsResumable() {
		resp, err = uploadManager.ResumeUploadFile(ctx, *resp.MultipartUploadResponse.UploadID)
		if err != nil {
			fmt.Println(resp)
		}
	}

	defer deleteObject(ctx, client, namespace, bname, objectName)
	fmt.Println("file uploaded")

}

func callBack(multiPartUploadPart transfer.MultiPartUploadPart) {
	if nil == multiPartUploadPart.Err {
		// Please refer this as the progress bar print content.
		// fmt.Printf("Part: %d / %d is uploaded.\n", multiPartUploadPart.PartNum, multiPartUploadPart.TotalParts)
		fmt.Printf("One example of progress bar could be the above comment content.\n")
		// Please refer following fmt to get each part opc-md5 res.
		// fmt.Printf("and this part opcMD5(64BasedEncoding) is: %s.\n", *multiPartUploadPart.OpcMD5 )
	}
}

/*
	Output:
	get namespace
	create bucket
	stream uploaded
	delete object
	delete bucket
*/
func ExampleObjectStorage_UploadManager_Stream() {
	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(common.CustomProfileConfigProvider(ociConfigFilePath, ociProfileName))
	helpers.FatalIfError(err)

	ctx := context.Background()
	bname := "bname"
	namespace := getNamespace(ctx, client)

	createBucket(ctx, client, namespace, bname)
	defer deleteBucket(ctx, client, namespace, bname)

	contentlen := 1024 * 1000 * 130 // 130MB
	filepath, _ := helpers.WriteTempFileOfSize(int64(contentlen))
	filename := path.Base(filepath)
	defer func() {
		os.Remove(filename)
	}()

	uploadManager := transfer.NewUploadManager()
	objectName := "sampleStreamUploadObj"

	file, _ := os.Open(filepath)
	defer file.Close()

	req := transfer.UploadStreamRequest{
		UploadRequest: transfer.UploadRequest{
			NamespaceName:                       common.String(namespace),
			BucketName:                          common.String(bname),
			ObjectName:                          common.String(objectName),
			EnableMultipartChecksumVerification: common.Bool(true),
		},
		StreamReader: file, // any struct implements the io.Reader interface
	}

	// if you want to overwrite default value, you can do it
	// as: transfer.UploadRequest.AllowMultipartUploads = common.Bool(false) // default is true
	// or: transfer.UploadRequest.AllowParallelUploads = common.Bool(false) // default is true
	_, err = uploadManager.UploadStream(context.Background(), req)

	if err != nil {
		fmt.Println(err)
	}

	defer deleteObject(ctx, client, namespace, bname, objectName)
	fmt.Println("stream uploaded")

}

func getNamespace(ctx context.Context, c objectstorage.ObjectStorageClient) string {
	request := objectstorage.GetNamespaceRequest{}
	r, err := c.GetNamespace(ctx, request)
	helpers.FatalIfError(err)
	fmt.Println("get namespace")
	return *r.Value
}

func putObject(ctx context.Context, c objectstorage.ObjectStorageClient, namespace, bucketname, objectname string, contentLen int64, content io.ReadCloser, metadata map[string]string) error {
	fmt.Println("Begin to put Object:", objectname)
	request := objectstorage.PutObjectRequest{
		NamespaceName: &namespace,
		BucketName:    &bucketname,
		ObjectName:    &objectname,
		ContentLength: &contentLen,
		PutObjectBody: content,
		OpcMeta:       metadata,
	}
	_, err := c.PutObject(ctx, request)
	fmt.Println("put object")
	return err
}

func deleteObject(ctx context.Context, c objectstorage.ObjectStorageClient, namespace, bucketname, objectname string) (err error) {
	request := objectstorage.DeleteObjectRequest{
		NamespaceName: &namespace,
		BucketName:    &bucketname,
		ObjectName:    &objectname,
	}
	_, err = c.DeleteObject(ctx, request)
	helpers.FatalIfError(err)
	fmt.Println("delete object")
	return
}

func createBucket(ctx context.Context, c objectstorage.ObjectStorageClient, namespace, name string) {
	fmt.Println("Begin to create bucket:", name)
	request := objectstorage.CreateBucketRequest{
		NamespaceName: &namespace,
	}

	request.CompartmentId = common.String(ociCompartmentId)
	fmt.Println("get request.CompartmentId:", *request.CompartmentId)
	request.Name = &name
	request.Metadata = make(map[string]string)
	request.PublicAccessType = objectstorage.CreateBucketDetailsPublicAccessTypeNopublicaccess
	_, err := c.CreateBucket(ctx, request)
	helpers.FatalIfError(err)

	fmt.Println("create bucket")
}

func deleteBucket(ctx context.Context, c objectstorage.ObjectStorageClient, namespace, name string) (err error) {
	request := objectstorage.DeleteBucketRequest{
		NamespaceName: &namespace,
		BucketName:    &name,
	}
	_, err = c.DeleteBucket(ctx, request)
	helpers.FatalIfError(err)

	fmt.Println("delete bucket")
	return
}

const ociConfigFilePath = "~/.oci/config"
const ociProfileName = "haiyouyou"
const ociCompartmentId = "ocid1.compartment.oc1..aaaaaaaarrh2qh5ehj226yge7k75o4uk6bo6g7eud4hszl7fw4fhbtv2v77q"

func main() {
	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(common.CustomProfileConfigProvider(ociConfigFilePath, ociProfileName))
	helpers.FatalIfError(err)
	req := objectstorage.GetNamespaceRequest{
		OpcClientRequestId: common.String(ociProfileName + "-opcClientRequestId"),
		CompartmentId:      common.String(ociCompartmentId),
	}
	// Send the request using the service client
	ctx := context.Background()
	resp, err := client.GetNamespace(ctx, req)
	helpers.FatalIfError(err)

	bname := "bname_test"
	namespace := *resp.Value
	//b, _ := json.Marshal(namespace)
	fmt.Println(namespace)

	createBucket(ctx, client, namespace, bname)
	defer deleteBucket(ctx, client, namespace, bname)

	contentlen := 1024 * 1000
	filepath, filesize := helpers.WriteTempFileOfSize(int64(contentlen))
	filename := path.Base(filepath)
	defer func() {
		os.Remove(filename)
	}()

	file, e := os.Open(filepath)
	if e != nil {
		helpers.FatalIfError(e)
	}
	defer file.Close()

	e = putObject(ctx, client, namespace, bname, filename, filesize, file, nil)
	helpers.FatalIfError(e)
	defer deleteObject(ctx, client, namespace, bname, filename)
}
