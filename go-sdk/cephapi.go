package main

import (
	"bufio"
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
	sysurl "net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"./dom4g"
	//"./go-logger/logger"
)

const (
	InitiateMultipartUploadResult = "InitiateMultipartUploadResult"
	TagListBucketResult           = "ListBucketResult"
)

type Content struct {
	Key          string    `json:"Key"`
	LastModified time.Time `json:"LastModified"`
	ETag         string    `json:"ETag"`
	Size         string    `json:"Size"`
	StorageClass string    `json:"StorageClass"`
	Owner        struct {
		ID          string `json:"ID"`
		DisplayName string `json:"DisplayName"`
	} `json:"Owner"`
}

type BucketList struct {
	MaxKeys     string    `json:"MaxKeys"`
	IsTruncated string    `json:"IsTruncated"`
	Contents    []Content `json:"Contents"`
	Xmlns       string    `json:"-xmlns"`
	Name        string    `json:"Name"`
	Prefix      string    `json:"Prefix"`
	Marker      string    `json:"Marker"`
}

type BucketListSingle struct {
	MaxKeys     string  `json:"MaxKeys"`
	IsTruncated string  `json:"IsTruncated"`
	Contents    Content `json:"Contents"`
	Xmlns       string  `json:"-xmlns"`
	Name        string  `json:"Name"`
	Prefix      string  `json:"Prefix"`
	Marker      string  `json:"Marker"`
}

type MultipartUpload struct {
	Bucket   string `json:"Bucket"`
	Key      string `json:"Key"`
	UploadID string `json:"UploadId"`
	Xmlns    string `json:"-xmlns"`
}

type Etagmap struct {
	EtagMutex sync.RWMutex
	Etag      map[string]string
}

var headerMutex sync.RWMutex

type AbstractS3API struct {
	Host        string
	AccessKey   string
	SecretKey   string
	Header      map[string]string
	MultiUpload MultipartUpload
	Etag        Etagmap
	Metadata    map[string]string
	LimitValue  int64
	Query       string
}

func (api *AbstractS3API) MakeCompleteXml() string {
	api.Etag.EtagMutex.RLock()
	el_complete := dom4g.NewElement("CompleteMultipartUpload", "")
	for strindex, etagvalue := range api.Etag.Etag {
		el_index := dom4g.NewElement("PartNumber", strindex)
		el_etag := dom4g.NewElement("ETag", etagvalue)

		el_part := dom4g.NewElement("Part", "")
		el_part.AddNode(el_index)
		el_part.AddNode(el_etag)

		el_complete.AddNode(el_part)
	}
	api.Etag.EtagMutex.RUnlock()
	return el_complete.ToString()
}

func (api *AbstractS3API) SetQuery(query string) {
	api.Query = query
}

func (api *AbstractS3API) SetEtag(key string, value string) {
	api.Etag.EtagMutex.Lock()
	api.Etag.Etag[key] = value
	api.Etag.EtagMutex.Unlock()
}

func (api *AbstractS3API) GetEtag(key string, value *string) {
	api.Etag.EtagMutex.RLock()
	v, ok := api.Etag.Etag[key]
	if ok {
		*value = v
	}
	api.Etag.EtagMutex.RUnlock()
}

func (api *AbstractS3API) SetLimitValue(value int64) {
	api.LimitValue = value
}

func (api *AbstractS3API) SetMultiUpload(value MultipartUpload) {
	api.MultiUpload = value
}

func (api *AbstractS3API) SetHeader(key string, value string) error {
	if strings.Contains(key, " ") || strings.Contains(value, " ") {
		return errors.New("Key and value mustn't contains blank space!")
	}
	api.Header[key] = value
	return nil
}

func (api *AbstractS3API) SetMetadata(key string, value string) error {
	if strings.Contains(key, " ") || strings.Contains(value, " ") {
		return errors.New("Key and value mustn't contains blank space!")
	}
	api.Metadata["X-Amz-Meta-"+strings.Title(key)] = value
	return nil
}

func (api *AbstractS3API) sortMetadataKeys() sort.StringSlice {
	keys := sort.StringSlice{}
	for key, _ := range api.Metadata {
		keys = append(keys, key)
	}
	keys.Sort()
	return keys
}

func (api *AbstractS3API) createSignString(requestMethod string, contentMd5 string, contentType string, requestDate string, url string, contentLength string) string {
	signString := requestMethod + "\n" + contentMd5 + "\n" + contentType + "\n" + requestDate + "\n"
	sortedKeys := api.sortMetadataKeys()
	for _, key := range sortedKeys {
		signString += fmt.Sprintf("%s:%s\n", strings.ToLower(key), api.Metadata[key])
	}
	headerMutex.RLock()
	for headerkey, headervalue := range api.Header {
		if strings.Contains(headerkey, "x-amz-") {
			sign := headerkey + ":" + headervalue + "\n"
			signString += sign
		}
	}
	headerMutex.RUnlock()

	signString += url
	return signString
}

func (api *AbstractS3API) createSign(requestMethod string, contentMd5 string, contentType string, requestDate string, url string, contentLength string) string {
	signString := api.createSignString(requestMethod, contentMd5, contentType, requestDate, url, contentLength)
	mac := hmac.New(sha1.New, []byte(api.SecretKey))
	mac.Write([]byte(signString))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func (api *AbstractS3API) OpenFile(filepath string) (*os.File, int64, *bufio.Reader) {
	fi, err := os.Open(filepath)
	if err != nil {
		fmt.Println("file open err:", err)
		return nil, int64(0), nil
	}

	st, _ := fi.Stat()
	data := bufio.NewReader(fi)
	return fi, int64(st.Size()), data
}

func (api *AbstractS3API) FileSize(filepath string) int64 {
	fi, err := os.Open(filepath)
	if err != nil {
		fmt.Println("file open err:", err)
		return int64(0)
	}
	defer fi.Close()
	st, err := fi.Stat()
	if err != nil {
		fmt.Println("file Stat err:", err)
		return int64(0)
	}
	return int64(st.Size())
}

func (api *AbstractS3API) Do(strurl string, method string, content string, isfile bool) (http.Header, string, error) {
	var contentlength int64
	respheader := http.Header{}
	resUri, pErr := sysurl.Parse(strurl)
	if pErr != nil {
		return respheader, "", nil
	}
	url := fmt.Sprintf("%s", resUri)

	if isfile {
		contentlength = api.FileSize(content)
	} else {
		contentlength = int64(len(content))
	}
	if contentlength > api.LimitValue {
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

	compurl := url + "?uploadId=" + api.MultiUpload.UploadID
	respheader, respdata, err = api._Do(compurl, "POST", completexml, false)
	if err != nil {
		fmt.Println("big file complete err:", err)
		return respheader, "", err
	}
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

		api.SetEtag(strindex, etagvalue)
	}

	waitgroup.Done()
	return respheader, "", nil
}

func (api *AbstractS3API) _DoBigPut(url string, method string, content string, isfile bool) (http.Header, string, error) {
	respheader := http.Header{}
	fi, err1 := os.Open(content)
	if err1 != nil {
		fmt.Println("file open err:", err1)
		return respheader, "", err1
	}
	defer fi.Close()
	st, err2 := fi.Stat()
	if err2 != nil {
		fmt.Println("file Stat err:", err2)
		return respheader, "", err2
	}
	filesize := int64(st.Size())
	splitnum := filesize/api.LimitValue + 1
	bufsize := filesize/splitnum + 1

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
			fmt.Println("ReadAt err:", err)
			return respheader, "", err
		}
		pos = pos + (int64)(n)

		waitgroup.Add(1)
		strindex := strconv.Itoa(index)
		parturl := url + "?partNumber=" + strindex + "&uploadId=" + api.MultiUpload.UploadID
		go api._DoBigPutPart(parturl, "PUT", string(read_buf), false, &waitgroup, strindex)
		index += 1
		if n == 0 || pos >= filesize {
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
		fmt.Println("InitiateMultipartUploadResult err:", err)
		return respheader, "", err
	} else {
		var multiUploadmap map[string]MultipartUpload
		err := json.Unmarshal([]byte(body), &multiUploadmap)
		if err != nil {
			fmt.Println("InitiateMultipartUploadResult Unmarshal err:", err, "body:", string(body))
			return respheader, "", err
		}
		value, ok := multiUploadmap[InitiateMultipartUploadResult]
		if ok {
			api.SetMultiUpload(value)
			fmt.Println("InitiateMultipartUploadResult:", value)
			return respheader, "", nil
		} else {
			fmt.Println("InitiateMultipartUploadResult is invalid")
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

	var requesturl string
	if strings.Contains(url, "?") {
		requesturl = api.Host + url + "&" + api.Query
	} else {
		if len(api.Query) != 0 {
			requesturl = api.Host + url + "?" + api.Query
		} else {
			requesturl = api.Host + url
		}
	}

	//request, err := http.NewRequest(method, api.Host+url, body)
	request, err := http.NewRequest(method, requesturl, body)
	if err != nil {
		fmt.Println("http.NewRequest err:", err)
		return respheader, "", err
	}
	requestDate := time.Now().UTC().Format("Mon, 2 Jan 2006 15:04:05 GMT")
	request.ContentLength = contentLength
	request.Header.Set("Date", requestDate)
	sign := api.createSign(method, "", "", requestDate, url, strsize)
	request.Header.Set("Authorization", "AWS "+api.AccessKey+":"+sign)
	request.Header.Set("Connection", "close")
	if len(strsize) != 0 {
		request.Header.Set("Content-Length", strsize)
	}

	for k, v := range api.Header {
		request.Header.Set(k, v)
	}

	client := http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout: 30 * time.Second,
			}).Dial,
			TLSHandshakeTimeout: 15 * time.Second,
		},
	}
	request.Close = true
	response, err := client.Do(request)

	if err != nil {
		fmt.Println("client.Do err:", err)
		return respheader, "", err
	}
	defer response.Body.Close()
	respdata, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println("ioutil.ReadAll err:", err)
		return respheader, "", err
	}
	respheader = response.Header
	return respheader, string(respdata), nil
}
