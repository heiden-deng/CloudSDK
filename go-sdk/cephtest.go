package main

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	tag_ListBucketResult = "ListBucketResult"
)

type BucketList struct {
	MaxKeys     string `json:"MaxKeys"`
	IsTruncated string `json:"IsTruncated"`
	Contents    []struct {
		Key          string    `json:"Key"`
		LastModified time.Time `json:"LastModified"`
		ETag         string    `json:"ETag"`
		Size         string    `json:"Size"`
		StorageClass string    `json:"StorageClass"`
		Owner        struct {
			ID          string `json:"ID"`
			DisplayName string `json:"DisplayName"`
		} `json:"Owner"`
	} `json:"Contents"`
	Xmlns  string `json:"-xmlns"`
	Name   string `json:"Name"`
	Prefix string `json:"Prefix"`
	Marker string `json:"Marker"`
}

func main() {
	//put_file_small()
	//put_file_big()
	//put_content()
	//get_xml()
	//get_json()
	//copy_object()
	//delete_object()
	//viewacl()
	//modifyacl()
	bucketlist()
}

func bucketlist() {
	header := map[string]string{}
	etag := etagmap{} //
	etag.etag = map[string]string{}
	multiUpload := MultipartUpload{}

	//api := AbstractS3API{"http://cos.speedycloud.org", "5C0FA427C421219C0D67FF372AB71784", "d519b8b1a9c0cc51100ccff69a3f574c87ba2969ab7f8a8f30d243a8d5d7d69b",
	//	header, multiUpload, etag, nil, 0}
	api := AbstractS3API{"http://cos.speedycloud.org", "28DDFEB01FD001BDE491F4C89401347C", "e05df3292e2ee10f75bba30b826042bcba48bc76f74cc1fd3d1f04425a7a5ec1",
		header, multiUpload, etag, nil, 0}
	api.SetHeader("Sc-Resp-Content-Type", "application/json")
	api.SetHeader("Accept-Encoding", "")
	isfile := false
	bucket := "/mofang-attachments"
	_, content, err := api.Do(bucket, "GET", "", isfile)
	if err != nil {
		fmt.Println("GET err:", err, "content:", content)
	} else {
		fmt.Println("GET success")
		listresult := map[string]BucketList{}
		err := json.Unmarshal([]byte(content), &listresult)
		if err != nil {
			fmt.Println("Unmarshal err:", err, "content:", content)
			return
		}
		fmt.Println("Unmarshal success")
		value, ok := listresult[tag_ListBucketResult]
		if !ok {
			fmt.Println("map have not key:", tag_ListBucketResult)
			return
		}
		contents := value.Contents
		sum := len(contents)
		fmt.Println("object sum:", sum)
		i := 0
		var aclurl string
		for i = 0; i < sum; i++ {
			///wangjiyou/wangjiyou.jpg?acl
			aclurl = bucket + "/" + contents[i].Key + "?acl"
			modifyacl(aclurl, i)
		}
	}
}

func modifyacl(url string, index int) {
	header := map[string]string{}
	etag := etagmap{} //
	etag.etag = map[string]string{}
	multiUpload := MultipartUpload{}

	//api := AbstractS3API{"http://cos.speedycloud.org", "5C0FA427C421219C0D67FF372AB71784", "d519b8b1a9c0cc51100ccff69a3f574c87ba2969ab7f8a8f30d243a8d5d7d69b",
	//	header, multiUpload, etag, nil, 0}
	api := AbstractS3API{"http://cos.speedycloud.org", "28DDFEB01FD001BDE491F4C89401347C", "e05df3292e2ee10f75bba30b826042bcba48bc76f74cc1fd3d1f04425a7a5ec1",
		header, multiUpload, etag, nil, 0}
	api.SetHeader("Sc-Resp-Content-Type", "application/json")
	api.SetHeader("Accept-Encoding", "")
	api.SetHeader("x-amz-acl", "public-read")
	isfile := false
	//_, content, err := api.Do("/wangjiyou/wangjiyou.jpg?acl", "PUT", "", isfile)
	_, content, err := api.Do(url, "PUT", "", isfile)
	if err != nil {
		fmt.Println("index:", index, " modifyacl url:", url, " err:", err, "content:", content)
	} else {
		fmt.Println("*********** index:", index, "  modifyacl url:", url, " success.content:", content, "*********")
	}
}

func viewacl() {
	header := map[string]string{}
	etag := etagmap{} //
	etag.etag = map[string]string{}
	multiUpload := MultipartUpload{}

	api := AbstractS3API{"http://cos.speedycloud.org", "5C0FA427C421219C0D67FF372AB71784", "d519b8b1a9c0cc51100ccff69a3f574c87ba2969ab7f8a8f30d243a8d5d7d69b",
		header, multiUpload, etag, nil, 0}
	api.SetHeader("Sc-Resp-Content-Type", "application/json")
	api.SetHeader("Accept-Encoding", "")
	//api.SetHeader("x-amz-acl", "public-read")
	isfile := false
	_, content, err := api.Do("/wangjiyou/wangjiyou.jpg?acl", "GET", "", isfile)
	if err != nil {
		fmt.Println("GET err:", err, "content:", content)
	} else {
		fmt.Println("GET success.content:", content)
	}
}

func delete_object() {
	header := map[string]string{}
	etag := etagmap{} //
	etag.etag = map[string]string{}
	multiUpload := MultipartUpload{}

	api := AbstractS3API{"http://172.16.10.200", "41A6839C70E2E842D3AB3C2B84BCECAB", "04b7cb09bc9be85888b245fee13d3e4e05096e29b83fc583dead9e5e550e16fc",
		header, multiUpload, etag, nil, 0}
	api.SetHeader("Sc-Resp-Content-Type", "application/json")
	api.SetHeader("Accept-Encoding", "")

	isfile := false
	_, content, err := api.Do("/wangjiyou/content.txt", "DELETE", "", isfile)
	if err != nil {
		fmt.Println("GET err:", err, "content:", content)
	} else {
		fmt.Println("GET success.content:", content)
	}
}

func copy_object() {
	header := map[string]string{}
	etag := etagmap{} //
	etag.etag = map[string]string{}
	multiUpload := MultipartUpload{}

	api := AbstractS3API{"http://172.16.10.200", "41A6839C70E2E842D3AB3C2B84BCECAB", "04b7cb09bc9be85888b245fee13d3e4e05096e29b83fc583dead9e5e550e16fc",
		header, multiUpload, etag, nil, 0}
	api.SetHeader("Sc-Resp-Content-Type", "application/json")
	api.SetHeader("Accept-Encoding", "")
	api.SetHeader("x-amz-copy-source", "/wangjiyou/a.mp4")
	isfile := false
	_, content, err := api.Do("/wangjiyou/a_copy.mp4", "PUT", "", isfile)
	if err != nil {
		fmt.Println("GET err:", err, "content:", content)
	} else {
		fmt.Println("GET success.content:", content)
	}
}

func get_json() {
	header := map[string]string{}
	etag := etagmap{} //
	etag.etag = map[string]string{}
	multiUpload := MultipartUpload{}

	api := AbstractS3API{"http://172.16.10.200", "41A6839C70E2E842D3AB3C2B84BCECAB", "04b7cb09bc9be85888b245fee13d3e4e05096e29b83fc583dead9e5e550e16fc",
		header, multiUpload, etag, nil, 0}
	api.SetHeader("Sc-Resp-Content-Type", "application/json")
	api.SetHeader("Accept-Encoding", "")

	isfile := false
	_, content, err := api.Do("/wangjiyou", "GET", "", isfile)
	if err != nil {
		fmt.Println("GET err:", err, "content:", content)
	} else {
		fmt.Println("GET success.content:", content)
	}
}

func get_xml() {
	header := map[string]string{}
	etag := etagmap{} //
	etag.etag = map[string]string{}
	multiUpload := MultipartUpload{}

	api := AbstractS3API{"http://172.16.10.200", "41A6839C70E2E842D3AB3C2B84BCECAB", "04b7cb09bc9be85888b245fee13d3e4e05096e29b83fc583dead9e5e550e16fc",
		header, multiUpload, etag, nil, 0}

	isfile := false
	_, content, err := api.Do("/wangjiyou", "GET", "", isfile)
	if err != nil {
		fmt.Println("PUT err:", err, "content:", content)
	} else {
		fmt.Println("PUT success.content:", content)
	}
}

func put_content() {
	header := map[string]string{}
	etag := etagmap{} //
	etag.etag = map[string]string{}
	multiUpload := MultipartUpload{}

	api := AbstractS3API{"http://172.16.10.200", "41A6839C70E2E842D3AB3C2B84BCECAB", "04b7cb09bc9be85888b245fee13d3e4e05096e29b83fc583dead9e5e550e16fc",
		header, multiUpload, etag, nil, 0}
	api.SetHeader("x-amz-acl", "public-read")
	var limit int64
	limit = int64(100 * 1024 * 1024)
	api.SetLimitValue(limit)
	isfile := false
	osfile := "/wangjiyou/content.txt"
	_, content, err := api.Do(osfile, "PUT", "/home/ying/a.mp4", isfile)
	if err != nil {
		fmt.Println("PUT err:", err, "content:", content)
	} else {
		fmt.Println("PUT success")
	}
}

func put_file_small() {
	header := map[string]string{}
	etag := etagmap{} //
	etag.etag = map[string]string{}
	multiUpload := MultipartUpload{}

	api := AbstractS3API{"http://172.16.10.200", "41A6839C70E2E842D3AB3C2B84BCECAB", "04b7cb09bc9be85888b245fee13d3e4e05096e29b83fc583dead9e5e550e16fc",
		header, multiUpload, etag, nil, 0}
	api.SetHeader("x-amz-acl", "public-read")
	var limit int64
	limit = int64(100 * 1024 * 1024)
	api.SetLimitValue(limit)
	isfile := true

	osfile := "/wangjiyou/030.flv"
	_, content, err := api.Do(osfile, "PUT", "/home/ying/030.flv", isfile)
	if err != nil {
		fmt.Println("PUT err:", err, "content:", content)
	} else {
		fmt.Println("PUT success")
	}
}

func put_file_big() {
	header := map[string]string{}
	etag := etagmap{} //
	etag.etag = map[string]string{}
	multiUpload := MultipartUpload{}

	api := AbstractS3API{"http://172.16.10.200", "41A6839C70E2E842D3AB3C2B84BCECAB", "04b7cb09bc9be85888b245fee13d3e4e05096e29b83fc583dead9e5e550e16fc",
		header, multiUpload, etag, nil, 0}
	api.SetHeader("x-amz-acl", "public-read")
	var limit int64
	limit = int64(100 * 1024 * 1024)
	api.SetLimitValue(limit)
	isfile := true

	osfile := "/wangjiyou/a.mp4"
	_, content, err := api.Do(osfile, "PUT", "/home/ying/a.mp4", isfile)
	if err != nil {
		fmt.Println("PUT err:", err, "content:", content)
	} else {
		fmt.Println("PUT success")
	}
}
