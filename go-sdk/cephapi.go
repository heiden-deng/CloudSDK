package main

import (
	"bufio"
	//"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"./dom4g"
	"./go-logger/logger"
)

const (
	InitiateMultipartUploadResult = "InitiateMultipartUploadResult"
)

type MultipartUpload struct {
	Bucket   string `json:"Bucket"`
	Key      string `json:"Key"`
	UploadID string `json:"UploadId"`
	Xmlns    string `json:"-xmlns"`
}

type etagmap struct {
	etagMutex sync.RWMutex
	etag      map[string]string
}

type AbstractS3API struct {
	host        string
	accessKey   string
	secretKey   string
	header      map[string]string
	multiUpload MultipartUpload
	etag        etagmap
	metadata    map[string]string
	limitValue  int64
}

//CompleteMultipartUpload
func (api *AbstractS3API) MakeCompleteXml() string {
	api.etag.etagMutex.RLock()
	el_complete := dom4g.NewElement("CompleteMultipartUpload", "")
	for strindex, etagvalue := range api.etag.etag {
		el_index := dom4g.NewElement("PartNumber", strindex)
		el_etag := dom4g.NewElement("ETag", etagvalue)

		el_part := dom4g.NewElement("Part", "")
		el_part.AddNode(el_index)
		el_part.AddNode(el_etag)

		el_complete.AddNode(el_part)
	}
	api.etag.etagMutex.RUnlock()
	return el_complete.ToString()
}

func (api *AbstractS3API) SetEtag(key string, value string) {
	api.etag.etagMutex.Lock()
	api.etag.etag[key] = value
	api.etag.etagMutex.Unlock()
}

func (api *AbstractS3API) GetEtag(key string, value *string) {
	api.etag.etagMutex.RLock()
	v, ok := api.etag.etag[key]
	if ok {
		*value = v
	}
	api.etag.etagMutex.RUnlock()
}

func (api *AbstractS3API) SetLimitValue(value int64) {
	api.limitValue = value
}

func (api *AbstractS3API) SetMultiUpload(value MultipartUpload) {
	api.multiUpload = value
}

func (api *AbstractS3API) SetHeader(key string, value string) error {
	if strings.Contains(key, " ") || strings.Contains(value, " ") {
		return errors.New("Key and value mustn't contains blank space!")
	}
	api.header[key] = value
	return nil
}

func (api *AbstractS3API) SetMetadata(key string, value string) error {
	if strings.Contains(key, " ") || strings.Contains(value, " ") {
		return errors.New("Key and value mustn't contains blank space!")
	}
	api.metadata["X-Amz-Meta-"+strings.Title(key)] = value
	return nil
}

func (api *AbstractS3API) sortMetadataKeys() sort.StringSlice {
	keys := sort.StringSlice{}
	for key, _ := range api.metadata {
		keys = append(keys, key)
	}
	keys.Sort()
	return keys
}

func (api *AbstractS3API) createSignString(requestMethod string, contentMd5 string, contentType string, requestDate string, url string, contentLength string) string {
	signString := requestMethod + "\n" + contentMd5 + "\n" + contentType + "\n" + requestDate + "\n"
	sortedKeys := api.sortMetadataKeys()
	for _, key := range sortedKeys {
		signString += fmt.Sprintf("%s:%s\n", strings.ToLower(key), api.metadata[key])
	}
	aclvalue, ok := api.header["x-amz-acl"]
	if ok {
		signacl := "x-amz-acl:" + aclvalue + "\n"
		signString += signacl
	}
	//signString += "x-amz-acl:public-read\n"
	signString += url
	//logger.Debug("sign:", signString)
	return signString
}

func (api *AbstractS3API) createSign(requestMethod string, contentMd5 string, contentType string, requestDate string, url string, contentLength string) string {
	signString := api.createSignString(requestMethod, contentMd5, contentType, requestDate, url, contentLength)
	mac := hmac.New(sha1.New, []byte(api.secretKey))
	mac.Write([]byte(signString))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func (api *AbstractS3API) OpenFile(filepath string) (*os.File, int64, *bufio.Reader) {
	fi, err := os.Open(filepath)
	if err != nil {
		logger.Debug("file open err:", err)
		return nil, int64(0), nil
	}

	st, _ := fi.Stat()
	data := bufio.NewReader(fi)
	return fi, int64(st.Size()), data
}

func (api *AbstractS3API) FileSize(filepath string) int64 {
	fi, err := os.Open(filepath)
	if err != nil {
		logger.Debug("file open err:", err)
		return int64(0)
	}
	defer fi.Close()
	st, err := fi.Stat()
	if err != nil {
		logger.Debug("file Stat err:", err)
		return int64(0)
	}
	return int64(st.Size())
}

func (api *AbstractS3API) Do(url string, method string, content string, isfile bool) (http.Header, string, error) {
	var contentlength int64
	respheader := http.Header{}
	if isfile {
		contentlength = api.FileSize(content)
	} else {
		contentlength = int64(len(content))
	}
	if contentlength > api.limitValue {
		return api._DoBig(url, method, content, isfile)
	} else {
		return api._Do(url, method, content, isfile)
	}
	return respheader, "", nil
}

func (api *AbstractS3API) _DoBig(url string, method string, content string, isfile bool) (http.Header, string, error) {
	respheader := http.Header{}
	var respdata string
	var err error
	respheader, _, err = api._DoBigInit(url, method, content, isfile)
	if err != nil {
		return respheader, "", err
	}

	respheader, _, err = api._DoBigPut(url, method, content, isfile)
	if err != nil {
		return respheader, "", err
	}
	respheader, respdata, err = api._DoBigComplete(url, method, content, isfile)
	if err != nil {
		return respheader, "", err
	}

	return respheader, respdata, nil
}

func (api *AbstractS3API) _DoBigComplete(url string, method string, _content string, isfile bool) (http.Header, string, error) {
	respheader := http.Header{}
	var respdata string
	var err error
	completexml := api.MakeCompleteXml()

	compurl := url + "?uploadId=" + api.multiUpload.UploadID
	respheader, respdata, err = api._Do(compurl, "POST", completexml, false)
	if err != nil {
		logger.Debug("big file complete err:", err)
		return respheader, "", err
	}
	//logger.Debug("big file complete respdata:", respdata)
	return respheader, respdata, nil
}

func (api *AbstractS3API) _DoBigPutPart(url string, method string, content string, isfile bool, waitgroup *sync.WaitGroup, _strindex string) (http.Header, string, error) {
	respheader := http.Header{}
	strindex := _strindex

	var err error
	respheader, _, err = api._Do(url, method, content, isfile)
	if err != nil {
		waitgroup.Done()
		return respheader, "", err
	}
	arretag, ok := respheader["Etag"]
	if ok {
		etagvalue := arretag[0]
		//logger.Debug("_DoBigPutPart ok url:", url, "index:", strindex, "etagvalue:", etagvalue, "content-length:", len(content))
		api.SetEtag(strindex, etagvalue)
	}

	waitgroup.Done()
	return respheader, "", nil
}

func (api *AbstractS3API) _DoBigPut(url string, method string, content string, isfile bool) (http.Header, string, error) {
	respheader := http.Header{}
	fi, err1 := os.Open(content)
	if err1 != nil {
		logger.Debug("file open err:", err1)
		return respheader, "", err1
	}
	defer fi.Close()
	st, err2 := fi.Stat()
	if err2 != nil {
		logger.Debug("file Stat err:", err2)
		return respheader, "", err2
	}
	filesize := int64(st.Size())
	splitnum := filesize/api.limitValue + 1
	bufsize := filesize/splitnum + 1
	//logger.Debug("filesize:", filesize, "api.limitValue:", api.limitValue, "bufsize:", bufsize)
	read_buf := make([]byte, bufsize)
	var pos int64 = 0
	var i int64
	var index int
	index = 1
	var waitgroup sync.WaitGroup
	for {

		for i = 0; i < bufsize; i++ {
			read_buf[i] = byte('\x00')
		}

		n, err := fi.ReadAt(read_buf, pos)
		if err != nil && err != io.EOF {
			//panic(err)
			logger.Debug("ReadAt err:", err)
			return respheader, "", err
		}
		pos = pos + (int64)(n)

		waitgroup.Add(1)
		strindex := strconv.Itoa(index)
		parturl := url + "?partNumber=" + strindex + "&uploadId=" + api.multiUpload.UploadID
		//logger.Debug("split file read_buf len:", len(read_buf))
		go api._DoBigPutPart(parturl, "PUT", string(read_buf), false, &waitgroup, strindex)
		index += 1
		if n == 0 || pos >= filesize {
			//logger.Debug("finish read")
			break
		}
	}
	waitgroup.Wait()
	return respheader, "", nil

}

func (api *AbstractS3API) _DoBigInit(url string, method string, content string, isfile bool) (http.Header, string, error) {
	respheader := http.Header{}
	api.SetHeader("Sc-Resp-Content-Type", "application/json")
	api.SetHeader("Accept-Encoding", "")
	_, body, err := api._Do(url+"?uploads", "POST", "", false)
	if err != nil {
		logger.Debug("InitiateMultipartUploadResult err:", err)
		return respheader, "", err
	} else {
		var multiUploadmap map[string]MultipartUpload
		err := json.Unmarshal([]byte(body), &multiUploadmap)
		if err != nil {
			logger.Debug("InitiateMultipartUploadResult Unmarshal err:", err, "body:", string(body))
			return respheader, "", err
		}
		value, ok := multiUploadmap[InitiateMultipartUploadResult]
		if ok {
			api.SetMultiUpload(value)
			logger.Debug("InitiateMultipartUploadResult:", value)
			return respheader, "", nil
		} else {
			logger.Debug("InitiateMultipartUploadResult is invalid")
			return respheader, "", errors.New("InitiateMultipartUploadResult is invalid")
		}
	}

	return respheader, "", nil
}

func (api *AbstractS3API) _Do(url string, method string, _content string, isfile bool) (http.Header, string, error) {
	content := _content
	var contentLength int64
	var body *bufio.Reader
	var strsize string
	var fi *os.File
	respheader := http.Header{}
	if isfile {
		fi, contentLength, body = api.OpenFile(content)
		if fi == nil {
			msg := content + " no such file or directory"
			return respheader, "", errors.New(msg)
		}
	} else {
		sr := strings.NewReader(content)
		body = bufio.NewReader(sr)
		contentLength = int64(len(content))
	}
	defer fi.Close()

	if contentLength == 0 {
		strsize = ""
	} else {
		strsize = strconv.FormatInt(int64(contentLength), 10)
	}

	//request, err := http.NewRequest("PUT", api.host+url, body)
	request, err := http.NewRequest(method, api.host+url, body)
	if err != nil {
		logger.Debug("http.NewRequest err:", err)
		return respheader, "", err
	}
	requestDate := time.Now().UTC().Format("Mon, 2 Jan 2006 15:04:05 GMT")
	request.ContentLength = contentLength
	request.Header.Set("Date", requestDate)
	sign := api.createSign(method, "", "", requestDate, url, strsize)
	request.Header.Set("Authorization", "AWS "+api.accessKey+":"+sign)
	request.Header.Set("Connection", "close")
	if len(strsize) != 0 {
		request.Header.Set("Content-Length", strsize)
	}

	for k, v := range api.header {
		request.Header.Set(k, v)
		//request.Header.Set("x-amz-acl", "public-read")
	}

	client := http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout: 30 * time.Second,
				//Deadline:  time.Now().Add(3 * time.Second),
				//KeepAlive: 2 * time.Second,
			}).Dial,
			TLSHandshakeTimeout: 15 * time.Second,
		},
		//Timeout: time.Duration(10) * time.Second,
	}
	request.Close = true
	response, err := client.Do(request)

	if err != nil {
		logger.Debug("client.Do err:", err)
		return respheader, "", err
	}
	defer response.Body.Close()
	respdata, err := ioutil.ReadAll(response.Body)
	if err != nil {
		logger.Debug("ioutil.ReadAll err:", err)
		return respheader, "", err
	}
	respheader = response.Header
	return respheader, string(respdata), nil
}

/*
func (api AbstractS3API) Get(url string) (string, error) {
	request, err := http.NewRequest("GET", api.host+url, nil)
	if err != nil {
		return "", err
	}
	requestDate := time.Now().UTC().Format("Mon, 2 Jan 2006 15:04:05 GMT")
	request.Header.Set("Date", requestDate)
	sign := api.createSign("GET", "", "", requestDate, url, "")
	request.Header.Set("Authorization", "AWS "+api.accessKey+":"+sign)
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	content, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func (api AbstractS3API) Put(url string, stream_addr string) (string, error) {
	fi, err := os.Open(stream_addr)
	if err != nil {
		logger.Debug("file open err:", err)
		return "", err
	}
	defer fi.Close()
	st, _ := fi.Stat()
	size := strconv.FormatInt(int64(st.Size()), 10)
	if err != nil {
		logger.Debug("open err:", err)
		return "", err
	}

	data := bufio.NewReader(fi)
	request, err := http.NewRequest("PUT", api.host+url, data)
	if err != nil {
		logger.Debug("http.NewRequest err:", err)
		return "", err
	}
	requestDate := time.Now().UTC().Format("Mon, 2 Jan 2006 15:04:05 GMT")
	request.ContentLength = st.Size()
	request.Header.Set("Date", requestDate)
	sign := api.createSign("PUT", "", "", requestDate, url, size)
	request.Header.Set("Authorization", "AWS "+api.accessKey+":"+sign)
	request.Header.Set("Content-Length", size)
	request.Header.Set("Connection", "close")
	request.Header.Set("x-amz-acl", "public-read")

	client := http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout:   2 * time.Second,
				Deadline:  time.Now().Add(3 * time.Second),
				KeepAlive: 2 * time.Second,
			}).Dial,
			TLSHandshakeTimeout: 2 * time.Second,
		},
		Timeout: time.Duration(10) * time.Second,
	}
	request.Close = true
	response, err := client.Do(request)

	if err != nil {
		logger.Debug("client.Do err:", err)
		return "", err
	}
	defer response.Body.Close()
	content, err := ioutil.ReadAll(response.Body)
	if err != nil {
		logger.Debug("ioutil.ReadAll err:", err)
		return "", err
	}
	return string(content), nil
}

func (api AbstractS3API) PutContent(url string, content string) (string, error) {
	sr := strings.NewReader(content)
	data := bufio.NewReader(sr)
	request, err := http.NewRequest("PUT", api.host+url, data)
	if err != nil {
		return "", err
	}
	size := strconv.FormatInt(int64(len(content)), 10)
	//size := int64(len(content))
	requestDate := time.Now().UTC().Format("Mon, 2 Jan 2006 15:04:05 GMT")
	request.ContentLength = int64(len(content))
	request.Header.Set("Date", requestDate)
	sign := api.createSign("PUT", "", "", requestDate, url, size)
	request.Header.Set("Authorization", "AWS "+api.accessKey+":"+sign)
	request.Header.Set("Content-Length", size)
	request.Header.Set("Connection", "close")
	request.Header.Set("x-amz-acl", "public-read")

	client := http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout:   2 * time.Second,
				Deadline:  time.Now().Add(3 * time.Second),
				KeepAlive: 2 * time.Second,
			}).Dial,
			TLSHandshakeTimeout: 2 * time.Second,
		},
		Timeout: time.Duration(10) * time.Second,
	}
	request.Close = true
	response, err := client.Do(request)

	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	result, err1 := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", err1
	}
	return string(result), nil
}

func (api AbstractS3API) PutTest(url string, stream_addr string) (string, error) {
	fi, err := ioutil.ReadFile(stream_addr)
	if err != nil {
		return "", err
	}
	buffer := bytes.NewBuffer(fi)
	size := "11"

	request, err := http.NewRequest("PUT", api.host+url, buffer)
	if err != nil {
		return "", err
	}
	requestDate := time.Now().UTC().Format("Mon, 2 Jan 2006 15:04:05 GMT")
	request.Header.Set("Date", requestDate)
	sign := api.createSign("PUT", "", "", requestDate, url, size)
	request.Header.Set("Authorization", "AWS "+api.accessKey+":"+sign)
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	content, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", err
	}
	return string(content), nil
}
*/
/*
func CephUpload(m3u8data M3u8MetaData) error {
	api := AbstractS3API{m3u8data.ObjectStoreVhost, m3u8data.Accesskey, m3u8data.Secretkey, nil}

	var err error
	if m3u8data.Flag == TagUploadContent {
		_, err = api.PutContent(m3u8data.ObjectStoreFileName, m3u8data.LocalUploadData)
	} else if m3u8data.Flag == TagUploadFile {
		_, err = api.Put(m3u8data.ObjectStoreFileName, m3u8data.LocalUploadData)
	}
	return err

}
*/
func test_main() {
	//sr := strings.NewReader("ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")
	//buf := bufio.NewReader(sr, 0)
	// api := AbstractS3API{"http://172.16.10.200", "41A6839C70E2E842D3AB3C2B84BCECAB", "04b7cb09bc9be85888b245fee13d3e4e05096e29b83fc583dead9e5e550e16fc", nil}
	// content, err := api.Get("/1.111")
	// if err != nil {
	// 	println(err)
	// } else {
	// 	println(content)
	// }
	//api := AbstractS3API{"http://172.16.10.200", "41A6839C70E2E842D3AB3C2B84BCECAB", "04b7cb09bc9be85888b245fee13d3e4e05096e29b83fc583dead9e5e550e16fc", nil}
	//content, err := api.Put("/1.111/hahahaha.flv", "/home/haopeng/hahahaha.flv")
	//content, err := api.PutTest("/1.111/hahahaha.flv", "/home/haopeng/hahahaha.flv")
	//if err != nil {
	//	println(err)
	//} else {
	//	println(content)
	//}
}
