package speedycloud;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public class Test {
    public static void main(String[] argc) throws UnsupportedEncodingException {
        //SpeedyCloudS3 s3api = new SpeedyCloudS3("705EE33BF78F96C80395184E78C024ED","020f25c3fcf27ccff0213dab8452595b7ffe4313fc14f636dff1a79b893a616c");
        //SpeedyCloudS3 s3api = new SpeedyCloudS3("http://118.119.254.216","8ECF99788044FA255AF79DD05451C450","df235c5664509dbe9c4971cdc7119ba3eb0228f1dae44a5e2df5cec378955b26");
    	SpeedyCloudS3 s3api = new SpeedyCloudS3("5C0FA427C421219C0D67FF372AB71784","d519b8b1a9c0cc51100ccff69a3f574c87ba2969ab7f8a8f30d243a8d5d7d69b");
        //String list = s3api.list("speedycloud");
        //System.out.println(list);
        
        //String delete = s3api.deleteBucket("course-pdf");
        //System.out.println(delete);
        //String createBucket = s3api.createBucket("test");
        //System.out.println(createBucket);
        //String putObjectFromFile(String bucket, String key, String path)
    	/**/
        
    	//String key = URLEncoder.encode("ubuntu.pdf", "utf-8");
        //String k1 = key;//key.replaceAll("\\+", "%20");
        //String setbucketacl = s3api.updateBucketAcl("test",  "public-read");
        //System.out.println("setbucketacl:"+setbucketacl);
        //String put = s3api.putObjectFromFile("code","timg.jpg","D:\\timg.jpg");
        //System.out.println(put);
        //String setkeyacl = s3api.updateKeyAcl("code", "timg.jpg", "public-read");
        //System.out.println(setkeyacl);
        int a = s3api.IsExsit("HEAD", "code","timg.jpg");
        System.out.println(a);
        
        //put = s3api.putObjectFromZipFile(bucket, key, path)
   /*     
    *     	String key = URLEncoder.encode("aa.txt", "utf-8");
        String k1 = key;//key.replaceAll("\\+", "%20");
        String put = s3api.putObjectFromFile("wangjiyou",k1,"D:\\Java\\jdk1.8.0_141\\LICENSE");
        System.out.println(put);
        String putString = s3api.putObjectFromString("wangjiyou","bb.txt","wangjiyou hahahahhahaha");
        System.out.println(putString);
        String setkeyacl = s3api.updateKeyAcl("wangjiyou", "bb.txt", "public-read");
        System.out.println(setkeyacl);
        String setbucketacl = s3api.updateBucketAcl("wangjiyou",  "public-read");
        System.out.println("setbucketacl:"+setbucketacl);
     */  
        
    }
}
