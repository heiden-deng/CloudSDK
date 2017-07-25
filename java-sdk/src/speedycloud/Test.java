package speedycloud;

public class Test {
    public static void main(String[] argc) {
        SpeedyCloudS3 s3api = new SpeedyCloudS3("705EE33BF78F96C80395184E78C024ED","020f25c3fcf27ccff0213dab8452595b7ffe4313fc14f636dff1a79b893a616c");
        String list = s3api.list("wangjiyou_test");
        System.out.println(list);
        //String delete = s3api.deleteBucket("course-pdf");
        //System.out.println(delete);
        //String createBucket = s3api.createBucket("course-pdf");
        //System.out.println(createBucket);
        //String putObjectFromFile(String bucket, String key, String path)
        String put = s3api.putObjectFromFile("wangjiyou_test","aa.txt","D:\\Java\\jdk1.8.0_141\\LICENSE");
        System.out.println(put);
        String putString = s3api.putObjectFromString("wangjiyou_test","bb.txt","wangjiyou hahahahhahaha");
        System.out.println(putString);
        String setkeyacl = s3api.updateKeyAcl("wangjiyou_test", "bb.txt", "public-read");
        System.out.println(setkeyacl);
        String setbucketacl = s3api.updateBucketAcl("wangjiyou_test",  "public-read");
        System.out.println("setbucketacl:"+setbucketacl);
    }
}
