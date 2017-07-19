import speedycloud.SpeedyCloudS3;
public class Test {
    public static void main(String[] argc) {
        SpeedyCloudS3 s3api = new SpeedyCloudS3("25E650EBAFDB46F7AD48719BE22BCA65","de4923209a333d1efc944720c8dbc3f608207030dd5c221e3aca6f27ffc3d47d");
        String list = s3api.list("frist");
        System.out.println(list);
        String delete = s3api.deleteBucket("course-pdf");
        System.out.println(delete);
        String createBucket = s3api.createBucket("course-pdf");
        System.out.println(createBucket);

    }
}

