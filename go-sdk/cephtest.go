package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"./dom4g"
	"./go-logger/logger"
)

func main() {
	logdir := GetCurrentDirectory() + "/log/"
	logger.SetConsole(true)
	logger.SetRollingDaily(logdir, "go-sdk.log")
	logger.SetLevel(logger.DEBUG)
	create_bucket()
	//put_file_small()
	//put_file_big()
	//put_content()
	//get_xml()
	//get_json()
	//copy_object()
	//delete_object()
	//viewacl()
	//modifyacl("/wangjiyou/bb.txt?acl", 1)
	//bucketlistandsetacl()
	//bucketlist()
	//parse_xml()
	//get_all_keys2()
}

func parse_xml() {
	/*
		s := `<a Age="10" go="1"><b>wu</b><c name="hi">xiao</c><d>dong</d><d>wxd</d><e><f>hello</f></e></a>`
		el, _ := dom4g.LoadByXml(s)
		//获取节点d 单个值
		//fmt.Println("d:", el.Node("d").Value)
		//获取节点d 多个值
		//el.
		fmt.Println("=========pre=========")
		for _, elem := range el.Nodes("d") {
			fmt.Println(elem.Name(), elem.Value)
		}
	*/

	s := `<?xml version="1.0" encoding="UTF-8"?>
		<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		    <Name>aaa</Name>
		    <Prefix></Prefix>
		    <Marker></Marker>
		    <MaxKeys>1000</MaxKeys>
		    <IsTruncated>false</IsTruncated>
		    <Contents>
		        <Key>boto3.pdf_.speedycloud_trash_flag</Key>
		        <LastModified>2017-09-11T03:34:58.966Z</LastModified>
		        <ETag>&quot;56f1189735baf84e914b72815c41c300&quot;</ETag>
		        <Size>10605522</Size>
		        <StorageClass>STANDARD</StorageClass>
		        <Owner>
		            <ID>user-1</ID>
		            <DisplayName>jiyou.wang</DisplayName>
		        </Owner>
		    </Contents>
		    <Contents>
		        <Key>cut.flv</Key>
		        <LastModified>2017-09-11T06:14:21.482Z</LastModified>
		        <ETag>&quot;21dcb8bb4aea259d7a15ee60f51ae914&quot;</ETag>
		        <Size>6203574</Size>
		        <StorageClass>STANDARD</StorageClass>
		        <Owner>
		            <ID>user-1</ID>
		            <DisplayName>jiyou.wang</DisplayName>
		        </Owner>
		    </Contents>
		</ListBucketResult>`
	//LoadByStream
	el, _ := dom4g.LoadByXml(s)
	el_complete := dom4g.NewElement("ListBucketResult", "")
	for _, attrs := range el.Attrs {
		el_complete.AddAttr(attrs.Name(), attrs.Value)
	}

	for _, elem := range el.Nodes("Contents") {
		if !strings.Contains(elem.ToString(), ".speedycloud_trash_flag") {
			//el_complete.AddNode(dom4g.NewElement("Contents", elem.ToString()))
			el_complete.AddNodeByString(elem.ToString())
		}
	}

	fmt.Println(el_complete.ToString())
}

func get_all_keys2() int {
	header := map[string]string{}
	etag := Etagmap{} //
	etag.Etag = map[string]string{}
	multiUpload := MultipartUpload{}

	api := AbstractS3API{"http://172.16.10.200", "41A6839C70E2E842D3AB3C2B84BCECAB", "04b7cb09bc9be85888b245fee13d3e4e05096e29b83fc583dead9e5e550e16fc", header, multiUpload, etag, nil, 0, ""}
	//api := AbstractS3API{"http://cos.speedycloud.org", "5C0FA427C421219C0D67FF372AB71784", "d519b8b1a9c0cc51100ccff69a3f574c87ba2969ab7f8a8f30d243a8d5d7d69b", header, multiUpload, etag, nil, 0, ""}
	//api := AbstractS3API{"http://cos.speedycloud.org", "28DDFEB01FD001BDE491F4C89401347C", "e05df3292e2ee10f75bba30b826042bcba48bc76f74cc1fd3d1f04425a7a5ec1", header, multiUpload, etag, nil, 0, ""}
	api.SetHeader("Sc-Resp-Content-Type", "application/json")
	api.SetHeader("Accept-Encoding", "")
	//api.SetQuery("max-keys=5&marker=0")
	//api.SetQuery("max-keys=10000")
	var marker string = ""
	var count int = 0
	for {
		query := fmt.Sprintf("max-keys=2&marker=%s", marker)

		api.SetQuery(query)
		isfile := false
		bucket := "/lifecycle_test"
		_, content, err := api.Do(bucket, "GET", "", isfile)
		if err != nil {
			logger.Debug("GET err:", err, "content:", content)
			break
		}
		logger.Debug("GET success")
		listresult := map[string]BucketList{}
		list_single_result := map[string]BucketListSingle{}
		err = json.Unmarshal([]byte(content), &listresult)
		if err != nil {
			err = json.Unmarshal([]byte(content), &list_single_result)
			if err != nil {
				logger.Debug("Unmarshal err:", err, "content:", content)
				break
			}
			//logger.Debug("object sum:", 1)
			value, ok := list_single_result[TagListBucketResult]
			if !ok {
				logger.Debug("map have not key:", TagListBucketResult, " content:", content)
				break
			}
			count += 1
			marker = value.Contents.Key
			continue
		}
		logger.Debug("Unmarshal success")
		value, ok := listresult[TagListBucketResult]
		if !ok {
			logger.Debug("map have not key:", TagListBucketResult, " content:", content)
			break
		}
		contents := value.Contents
		sum := len(contents)
		logger.Debug("object sum:", sum)
		if sum == 0 {
			break
		}
		/*
			i := 0
			var aclurl string
			for i = 0; i < sum; i++ {
				///wangjiyou/wangjiyou.jpg?acl
				aclurl = bucket + "/" + contents[i].Key + "?acl"
				//modifyacl(aclurl, i)
				logger.Debug("object name:", aclurl)
			}
		*/
		count += sum
		marker = contents[sum-1].Key

	}
	logger.Debug("count :", count)
	return 0
}

func bucketlist() {
	header := map[string]string{}
	etag := Etagmap{} //
	etag.Etag = map[string]string{}
	multiUpload := MultipartUpload{}

	api := AbstractS3API{"http://172.16.10.200", "41A6839C70E2E842D3AB3C2B84BCECAB", "04b7cb09bc9be85888b245fee13d3e4e05096e29b83fc583dead9e5e550e16fc", header, multiUpload, etag, nil, 0, ""}
	//api := AbstractS3API{"http://cos.speedycloud.org", "5C0FA427C421219C0D67FF372AB71784", "d519b8b1a9c0cc51100ccff69a3f574c87ba2969ab7f8a8f30d243a8d5d7d69b", header, multiUpload, etag, nil, 0, ""}
	//api := AbstractS3API{"http://cos.speedycloud.org", "28DDFEB01FD001BDE491F4C89401347C", "e05df3292e2ee10f75bba30b826042bcba48bc76f74cc1fd3d1f04425a7a5ec1",
	//	header, multiUpload, etag, nil, 0, ""}
	api.SetHeader("Sc-Resp-Content-Type", "application/json")
	api.SetHeader("Accept-Encoding", "")
	api.SetQuery("max-keys=10000")
	isfile := false
	bucket := "/wangjiyou"
	_, content, err := api.Do(bucket, "GET", "", isfile)
	if err != nil {
		logger.Debug("GET err:", err, "content:", content)
	} else {
		logger.Debug("GET success")
		listresult := map[string]BucketList{}
		err := json.Unmarshal([]byte(content), &listresult)
		if err != nil {
			logger.Debug("Unmarshal err:", err, "content:", content)
			return
		}
		logger.Debug("Unmarshal success")
		value, ok := listresult[TagListBucketResult]
		if !ok {
			logger.Debug("map have not key:", TagListBucketResult, " content:", content)
			return
		}
		contents := value.Contents
		sum := len(contents)
		logger.Debug("object sum:", sum)
		/*
			i := 0
			var aclurl string
			for i = 0; i < sum; i++ {
				///wangjiyou/wangjiyou.jpg?acl
				aclurl = bucket + "/" + contents[i].Key + "?acl"
				logger.Debug("object name:", aclurl)
			}
		*/
	}
}

func bucketlistandsetacl() {
	header := map[string]string{}
	etag := Etagmap{} //
	etag.Etag = map[string]string{}
	multiUpload := MultipartUpload{}

	//api := AbstractS3API{"http://cos.speedycloud.org", "5C0FA427C421219C0D67FF372AB71784", "d519b8b1a9c0cc51100ccff69a3f574c87ba2969ab7f8a8f30d243a8d5d7d69b",
	//	header, multiUpload, etag, nil, 0}
	api := AbstractS3API{"http://cos.speedycloud.org", "28DDFEB01FD001BDE491F4C89401347C", "e05df3292e2ee10f75bba30b826042bcba48bc76f74cc1fd3d1f04425a7a5ec1",
		header, multiUpload, etag, nil, 0, ""}
	api.SetHeader("Sc-Resp-Content-Type", "application/json")
	api.SetHeader("Accept-Encoding", "")
	api.SetQuery("max-keys=10000")
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
		value, ok := listresult[TagListBucketResult]
		if !ok {
			fmt.Println("map have not key:", TagListBucketResult, " content:", content)
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
	etag := Etagmap{} //
	etag.Etag = map[string]string{}
	multiUpload := MultipartUpload{}

	ak := "8ECF99788044FA255AF79DD05451C450"
	sk := "df235c5664509dbe9c4971cdc7119ba3eb0228f1dae44a5e2df5cec378955b26"
	api := AbstractS3API{"http://118.119.254.216", ak, sk,
		header, multiUpload, etag, nil, 0, ""}
	api.SetHeader("Sc-Resp-Content-Type", "application/json")
	api.SetHeader("Accept-Encoding", "")
	api.SetHeader("x-amz-acl", "public-read")
	isfile := false
	//_, content, err := api.Do("/wangjiyou/wangjiyou.jpg?acl", "PUT", "", isfile)
	_, content, err := api.Do(url, "PUT", "", isfile)
	if err != nil {
		logger.Debug("index:", index, " modifyacl url:", url, " err:", err, "content:", content)
	} else {
		logger.Debug("*********** index:", index, "  modifyacl url:", url, " success.content:", content, "*********")
	}
}

func viewacl() {
	header := map[string]string{}
	etag := Etagmap{} //
	etag.Etag = map[string]string{}
	multiUpload := MultipartUpload{}

	api := AbstractS3API{"http://cos.speedycloud.org", "5C0FA427C421219C0D67FF372AB71784", "d519b8b1a9c0cc51100ccff69a3f574c87ba2969ab7f8a8f30d243a8d5d7d69b",
		header, multiUpload, etag, nil, 0, ""}
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
	etag := Etagmap{} //
	etag.Etag = map[string]string{}
	multiUpload := MultipartUpload{}

	api := AbstractS3API{"http://172.16.10.200", "41A6839C70E2E842D3AB3C2B84BCECAB", "04b7cb09bc9be85888b245fee13d3e4e05096e29b83fc583dead9e5e550e16fc",
		header, multiUpload, etag, nil, 0, ""}
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
	etag := Etagmap{} //
	etag.Etag = map[string]string{}
	multiUpload := MultipartUpload{}

	api := AbstractS3API{"http://172.16.10.200", "41A6839C70E2E842D3AB3C2B84BCECAB", "04b7cb09bc9be85888b245fee13d3e4e05096e29b83fc583dead9e5e550e16fc",
		header, multiUpload, etag, nil, 0, ""}
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
	etag := Etagmap{} //
	etag.Etag = map[string]string{}
	multiUpload := MultipartUpload{}

	api := AbstractS3API{"http://172.16.10.200", "41A6839C70E2E842D3AB3C2B84BCECAB", "04b7cb09bc9be85888b245fee13d3e4e05096e29b83fc583dead9e5e550e16fc",
		header, multiUpload, etag, nil, 0, ""}
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
	etag := Etagmap{} //
	etag.Etag = map[string]string{}
	multiUpload := MultipartUpload{}

	api := AbstractS3API{"http://172.16.10.200", "41A6839C70E2E842D3AB3C2B84BCECAB", "04b7cb09bc9be85888b245fee13d3e4e05096e29b83fc583dead9e5e550e16fc",
		header, multiUpload, etag, nil, 0, ""}

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
	etag := Etagmap{} //
	etag.Etag = map[string]string{}
	multiUpload := MultipartUpload{}

	api := AbstractS3API{"http://172.16.10.200", "41A6839C70E2E842D3AB3C2B84BCECAB", "04b7cb09bc9be85888b245fee13d3e4e05096e29b83fc583dead9e5e550e16fc",
		header, multiUpload, etag, nil, 0, ""}
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
	etag := Etagmap{} //
	etag.Etag = map[string]string{}
	multiUpload := MultipartUpload{}
	sk := "df235c5664509dbe9c4971cdc7119ba3eb0228f1dae44a5e2df5cec378955b27"
	api := AbstractS3API{"http://103.235.221.75", "8ECF99788044FA255AF79DD05451C451", sk,
		header, multiUpload, etag, nil, 0, ""}
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
	etag := Etagmap{} //
	etag.Etag = map[string]string{}
	multiUpload := MultipartUpload{}

	api := AbstractS3API{"http://172.16.10.200", "41A6839C70E2E842D3AB3C2B84BCECAB", "04b7cb09bc9be85888b245fee13d3e4e05096e29b83fc583dead9e5e550e16fc",
		header, multiUpload, etag, nil, 0, ""}
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

func create_bucket() {
	header := map[string]string{}
	etag := Etagmap{} //
	etag.Etag = map[string]string{}
	multiUpload := MultipartUpload{}
	ak := "5C0FA427C421219C0D67FF372AB71784"
	sk := "d519b8b1a9c0cc51100ccff69a3f574c87ba2969ab7f8a8f30d243a8d5d7d69b"
	api := AbstractS3API{"http://oss-cn-beijing.speedycloud.org", ak, sk,
		header, multiUpload, etag, nil, 0, ""}
	api.SetHeader("x-amz-acl", "public-read")
	var limit int64
	limit = int64(100 * 1024 * 1024)
	api.SetLimitValue(limit)
	isfile := false

	osfile := "/wangjiyou-test"
	_, content, err := api.Do(osfile, "PUT", "", isfile)
	if err != nil {
		fmt.Println("PUT err:", err, "content:", content)
	} else {
		fmt.Println("PUT success")
	}
}

func GetCurrentDirectory() string {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		return ""
	}
	return strings.Replace(dir, "\\", "/", -1)
}
