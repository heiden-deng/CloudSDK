package speedycloud;

/*
        url: 已上传到对象存储的对象的ur了（必填）
        address: 房源的地址
        bucket: 目标桶
        host: 目标桶的host

 * */

public class video_source {
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	public String getBucket() {
		return bucket;
	}
	public void setBucket(String bucket) {
		this.bucket = bucket;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	private String url;
	private String address;
	private String bucket;
	private String host;

}