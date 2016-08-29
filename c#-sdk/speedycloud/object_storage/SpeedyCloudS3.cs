using System;
namespace object_storage
{
    class SpeedyCloudS3 : AbstractProductAPI
    {
        public SpeedyCloudS3(string access_key, string secret_key) : base(access_key, secret_key)
        {

        }

        static void Main(string[] args)
        {
            SpeedyCloudS3 s3 = new SpeedyCloudS3("41A6839C70E2E842D3AB3C2B84BCECAB", "04b7cb09bc9be85888b245fee13d3e4e05096e29b83fc583dead9e5e550e16fc");
            Console.WriteLine(s3.putObjectFromFile("haha", "heihei", @"c:\Logs\Application.evtx"));
            //Console.WriteLine(s3.putObjectFromString("haha", "hehe", "baka"));
            Console.WriteLine(s3.updateKeyAcl("haha", "heihei", "public-read"));
            Console.ReadKey();
        }

        public string list(string bucket)
        {
            return this.request("GET", "/" + bucket);
        }

        public string createBucket(string bucket)
        {
            return this.request("PUT", "/" + bucket);
        }

        public string deleteBucket(string bucket)
        {
            return this.request("DELETE", "/" + bucket);
        }

        public string queryBucketAcl(string bucket)
        {
            return this.request("GET", string.Format("/{0}?acl", bucket));
        }

        public string queryObjectAcl(string bucket, string key)
        {
            return this.request("GET", string.Format("/{0}/{1}?acl", bucket, key));
        }

        public string deleteKey(string bucket, string key)
        {
            return this.request("DELETE", string.Format("/{0}/{1}", bucket, key));
        }

        public string deleteVersioningKey(string bucket, string key, string versionId)
        {
            return this.request("DELETE", string.Format("/{0}/{1}?versionId={2}", bucket, key, versionId));
        }

        public string getKeyVersions(string bucket)
        {
            return this.request("GET", string.Format("/{0}?versions", bucket));
        }

        public string configureBucketVersioning(string bucket, string status)
        {
            string path = string.Format("/{0}?versioning", bucket);
            string versioningBody = string.Format("<?xml version=\"1.0\"encoding=\"UTF-8\"?><VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>{0}</Status ></VersioningConfiguration>", status);
            return this.putString("PUT", path, versioningBody);
        }

        public string getBucketVersioningStatus(string bucket)
        {
            return this.request("GET", string.Format("/{0}?versioning", bucket));
        }

        public string putObjectFromFile(string bucket, string key, string path)
        {
            return this.putKeyFromFile("PUT", string.Format("/{0}/{1}", bucket, key), path);
        }

        public string putObjectFromString(string bucket, string key, string s)
        {
            return this.putKeyFromString("PUT", string.Format("/{0}/{1}", bucket, key), s);
        }

        public string updateBucketAcl(string bucket, string key, string acl)
        {
            return this.requestUpdate("PUT", string.Format("/{0}?acl", bucket), acl);
        }

        public string updateKeyAcl(string bucket, string key, string acl)
        {
            return this.requestUpdate("PUT", string.Format("/{0}/{1}?acl", bucket, key), acl);
        }

        public string updateVersioningKeyAcl(string bucket, string key, string versionId, string acl)
        {
            return this.requestUpdate("PUT", string.Format("/{0}/{1}?acl&versionId={2}", bucket, key, versionId), acl);
        }
    }
}
