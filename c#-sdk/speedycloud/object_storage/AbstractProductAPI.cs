using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Security.Cryptography;
using System.Text;

namespace object_storage
{
    class AbstractProductAPI
    {
        private string access_key;
        private string secret_key;
        private string host;
        private SortedList<string, string> metadata;

        public AbstractProductAPI(string accesskey, string secretkey)
        {
            this.host = "http://osc.speedycloud.net";
            this.access_key = accesskey;
            this.secret_key = secretkey;
            this.metadata = new SortedList<string, string>();
        }

        private string createSignString(HttpWebRequest request, string path)
        {
            string sign = request.Method;
            sign += "\n" + request.Headers.Get("Content-Md5");
            sign += "\n" + request.ContentType;
            string date = request.Date.AddHours(-8).GetDateTimeFormats('r')[0].ToString();
            sign += "\n" + date;
            foreach (var entry in metadata)
            {
                sign += "\n" + entry.Key.ToLower() + ":" + entry.Value;
            }
            sign += "\n" + path;
            return sign;
        }

        private string createSign(HttpWebRequest request, string path)
        {
            HMACSHA1 hmacsha1 = new HMACSHA1();
            hmacsha1.Initialize();
            hmacsha1.Key = Encoding.Default.GetBytes(this.secret_key);
            byte[] final = hmacsha1.ComputeHash(Encoding.Default.GetBytes(createSignString(request, path)));
            return Convert.ToBase64String(final);
        }

        public string request(string method, string path)
        {
            string serverURL = this.host + path;
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(serverURL);
            foreach (var entry in metadata)
            {
                request.Headers[entry.Key] = entry.Value;
            }
            request.Method = method;
            request.Date = DateTime.UtcNow;
            request.Headers.Add("Authorization", "AWS " + this.access_key + ":" + createSign(request, path));
            return this.returnResponse(request); ;
        }

        public string putData(string method, string url, string data, string type)
        {
            string serverURL = this.host + url;
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(serverURL);
            foreach (var entry in metadata)
            {
                request.Headers[entry.Key] = entry.Value;
            }
            request.Method = method;
            request.Date = DateTime.UtcNow;
            MD5 md5 = new MD5CryptoServiceProvider();
            if (type == "string")
            {
                request.ContentType = "text/plain";
                request.ContentLength = data.Length;
                byte[] result = Encoding.Default.GetBytes(data);
                byte[] output = md5.ComputeHash(result);
                request.Headers.Set("Content-Md5", Convert.ToBase64String(output));
                request.Headers.Add("Authorization", "AWS " + this.access_key + ":" + createSign(request, url));
                Stream stream = request.GetRequestStream();
                stream.Write(result, 0, result.Length);
            }
            else if (type == "file")
            {
                FileStream fileStream = File.OpenRead(data);
                byte[] output = md5.ComputeHash(fileStream);
                request.ContentLength = fileStream.Length;
                fileStream.Close();
                request.Headers.Set("Content-Md5", Convert.ToBase64String(output));
                request.Headers.Add("Authorization", "AWS " + this.access_key + ":" + createSign(request, url));
                Stream stream = request.GetRequestStream();
                byte[] buffer = new byte[4096];
                FileStream fs = File.OpenRead(data);
                while (fs.Read(buffer, 0, 4096) > 0)
                {
                    stream.Write(buffer, 0, 4096);
                    stream.Flush();
                }
                fs.Close();
            }
            else
            {
                return "";
            }
            return this.returnResponse(request);
        }

        public string putKeyFromFile(string method, string url, string path)
        {
            return putData(method, url, path, "file");
        }

        public string putKeyFromString(string method, string url, string requestString)
        {
            return putData(method, url, requestString, "string");
        }

        public string putString(string method, string url, string requestString)
        {
            return putData(method, url, requestString, "string");
        }

        public string requestUpdate(string method, string url, string acl)
        {
            string serverURL = this.host + url;
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(serverURL);
            this.metadata.Add("X-Amz-Acl", acl);
            foreach (var entry in metadata)
            {
                request.Headers[entry.Key] = entry.Value;
            }
            request.Method = method;
            request.Date = DateTime.UtcNow;
            request.Headers.Add("Authorization", "AWS " + this.access_key + ":" + createSign(request, url));
            return this.returnResponse(request);
        }

        private string returnResponse(HttpWebRequest request)
        {
            HttpWebResponse response = (HttpWebResponse)request.GetResponse();
            Stream streamResponse = response.GetResponseStream();
            StreamReader streamReader = new StreamReader(streamResponse, Encoding.UTF8);
            string content = "";
            string line;
            while ((line = streamReader.ReadLine()) != null)
            {
                content += line;
            }
            streamResponse.Close();
            streamReader.Close();
            response.Close();
            return content;
        }
    }
}
