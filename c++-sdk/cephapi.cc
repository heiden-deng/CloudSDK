#include "libs3.h"
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>



S3Status responsePropertiesCallback(
                const S3ResponseProperties *properties,
                void *callbackData)
{
        return S3StatusOK;
}

static void responseCompleteCallback(
                S3Status status,
                const S3ErrorDetails *error,
                void *callbackData)
{
        return;
}

S3ResponseHandler responseHandler =
{
        &responsePropertiesCallback,
        &responseCompleteCallback
};



static S3Status listBucketCallback(
    int isTruncated,
    const char *nextMarker,
    int contentsCount,
    const S3ListBucketContent *contents,
    int commonPrefixesCount,
    const char **commonPrefixes,
    void *callbackData)
{
    printf("%-22s", "      Object Name");
    printf("  %-5s  %-20s", "Size", "   Last Modified");
    printf("\n");
    printf("----------------------");
    printf("  -----" "  --------------------");
    printf("\n");

    for (int i = 0; i < contentsCount; i++) {
        char timebuf[256];
        char sizebuf[16];
        const S3ListBucketContent *content = &(contents[i]);
        time_t t = (time_t) content->lastModified;

        strftime(timebuf, sizeof(timebuf), "%Y-%m-%dT%H:%M:%SZ", gmtime(&t));
        sprintf(sizebuf, "%5llu", (unsigned long long) content->size);
        printf("%-22s  %s  %s\n", content->key, sizebuf, timebuf);
    }

    return S3StatusOK;
}

S3ListBucketHandler listBucketHandler =
{
        responseHandler,
        &listBucketCallback
};




S3BucketContext bucketContext =
{
        "cos.speedycloud.org",
        "wangjiyou",
        S3ProtocolHTTP,
        S3UriStylePath,
        "5C0FA427C421219C0D67FF372AB71784",
        "d519b8b1a9c0cc51100ccff69a3f574c87ba2969ab7f8a8f30d243a8d5d7d69b"
};

int main(int argc, char *argv[])
{
    S3_initialize("s3", S3_INIT_ALL, "cos.speedycloud.org");
    S3_list_bucket(&bucketContext, NULL, NULL, NULL, 0, NULL, 
                   &listBucketHandler, NULL);
    S3_deinitialize();
    return 0;
}
