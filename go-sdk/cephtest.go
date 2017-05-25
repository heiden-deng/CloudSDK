package main

import (
	"./go-logger/logger"
)

func main() {
	//put_file()
	//put_content()
	//get_xml()
	get_json()
}

func get_json() {
	header := map[string]string{}
	etag := etagmap{} //
	etag.etag = map[string]string{}
	multiUpload := MultipartUpload{}

	api := AbstractS3API{"http://172.16.10.200", "41A6839C70E2E842D3AB3C2B84BCECAB", "04b7cb09bc9be85888b245fee13d3e4e05096e29b83fc583dead9e5e550e16fc",
		header, multiUpload, etag, nil, 0}
	//api.SetHeader("x-amz-acl", "public-read")
	api.SetHeader("Sc-Resp-Content-Type", "application/json")
	api.SetHeader("Accept-Encoding", "")

	isfile := false
	_, content, err := api.Do("/wangjiyou", "GET", "", isfile)
	if err != nil {
		logger.Debug("GET err:", err, "content:", content)
	} else {
		logger.Debug("GET success.content:", content)
	}
}

func get_xml() {
	header := map[string]string{}
	etag := etagmap{} //
	etag.etag = map[string]string{}
	multiUpload := MultipartUpload{}

	api := AbstractS3API{"http://172.16.10.200", "41A6839C70E2E842D3AB3C2B84BCECAB", "04b7cb09bc9be85888b245fee13d3e4e05096e29b83fc583dead9e5e550e16fc",
		header, multiUpload, etag, nil, 0}
	//api.SetHeader("x-amz-acl", "public-read")

	isfile := false
	_, content, err := api.Do("/wangjiyou", "GET", "", isfile)
	if err != nil {
		logger.Debug("PUT err:", err, "content:", content)
	} else {
		logger.Debug("PUT success.content:", content)
	}
}

func put_content() {
	header := map[string]string{}
	etag := etagmap{} //
	etag.etag = map[string]string{}
	multiUpload := MultipartUpload{}

	//api := AbstractS3API{conf.ObjectStoreVhost, "5C0FA427C421219C0D67FF372AB71784", "d519b8b1a9c0cc51100ccff69a3f574c87ba2969ab7f8a8f30d243a8d5d7d69b", "", header, etag, nil}
	//api := AbstractS3API{"http://172.16.10.200", "41A6839C70E2E842D3AB3C2B84BCECAB", "04b7cb09bc9be85888b245fee13d3e4e05096e29b83fc583dead9e5e550e16fc", header, multiUpload, etag, nil, 0}
	//api := AbstractS3API{zeadata.OSHost, zeadata.Accesskey, zeadata.Secretkey, header, multiUpload, etag, nil, 0}
	api := AbstractS3API{"http://172.16.10.200", "41A6839C70E2E842D3AB3C2B84BCECAB", "04b7cb09bc9be85888b245fee13d3e4e05096e29b83fc583dead9e5e550e16fc",
		header, multiUpload, etag, nil, 0}
	api.SetHeader("x-amz-acl", "public-read")
	var limit int64
	limit = int64(100 * 1024 * 1024)
	api.SetLimitValue(limit)
	isfile := false
	//_, content, err := api.Do("/appid1/d.txt", "PUT", "/home/ying/a.mp4", isfile)
	osfile := "/wangjiyou/content.txt"
	_, content, err := api.Do(osfile, "PUT", "/home/ying/a.mp4", isfile)
	if err != nil {
		logger.Debug("PUT err:", err, "content:", content)
	} else {
		logger.Debug("PUT success")
	}
}

func put_file() {
	header := map[string]string{}
	etag := etagmap{} //
	etag.etag = map[string]string{}
	multiUpload := MultipartUpload{}

	//api := AbstractS3API{conf.ObjectStoreVhost, "5C0FA427C421219C0D67FF372AB71784", "d519b8b1a9c0cc51100ccff69a3f574c87ba2969ab7f8a8f30d243a8d5d7d69b", "", header, etag, nil}
	//api := AbstractS3API{"http://172.16.10.200", "41A6839C70E2E842D3AB3C2B84BCECAB", "04b7cb09bc9be85888b245fee13d3e4e05096e29b83fc583dead9e5e550e16fc", header, multiUpload, etag, nil, 0}
	//api := AbstractS3API{zeadata.OSHost, zeadata.Accesskey, zeadata.Secretkey, header, multiUpload, etag, nil, 0}
	api := AbstractS3API{"http://172.16.10.200", "41A6839C70E2E842D3AB3C2B84BCECAB", "04b7cb09bc9be85888b245fee13d3e4e05096e29b83fc583dead9e5e550e16fc",
		header, multiUpload, etag, nil, 0}
	api.SetHeader("x-amz-acl", "public-read")
	var limit int64
	limit = int64(100 * 1024 * 1024)
	api.SetLimitValue(limit)
	isfile := true

	osfile := "/wangjiyou/a.mp4"
	_, content, err := api.Do(osfile, "PUT", "/home/ying/030.flv", isfile)
	if err != nil {
		logger.Debug("PUT err:", err, "content:", content)
	} else {
		logger.Debug("PUT success")
	}
}
