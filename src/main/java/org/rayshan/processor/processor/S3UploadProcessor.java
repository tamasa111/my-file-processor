package org.rayshan.processor.processor;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.rayshan.processor.config.AppConfig;
import org.rayshan.processor.model.FileInfo;
import org.rayshan.processor.model.FileIngestedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;

@ApplicationScoped
public class S3UploadProcessor implements Processor {
    private final Logger LOG = LoggerFactory.getLogger(S3UploadProcessor.class);

    @Inject
    AppConfig config;

    @Override
    public void process(Exchange exchange) throws Exception {
        FileIngestedEvent ingestedEvent = exchange.getProperty("ingestedEvent", FileIngestedEvent.class);
        FileInfo fileInfo = ingestedEvent.getFile();
        
        String bucketName = exchange.getIn().getHeader("CamelAwsS3BucketName", String.class);
        String key = exchange.getIn().getHeader("CamelAwsS3Key", String.class);
        
        if (bucketName == null) {
            bucketName = fileInfo.getIngestedS3Bucket();
        }
        
        byte[] body = exchange.getIn().getBody(byte[].class);
        if (body == null) {
            LOG.warn("No body to upload to S3");
            return;
        }

        S3Client s3Client = createS3Client();
        try {
            PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentLength((long) body.length)
                    .build();

            s3Client.putObject(putRequest, RequestBody.fromBytes(body));
            LOG.info("Uploaded {} bytes to s3://{}/{}", body.length, bucketName, key);
        } finally {
            s3Client.close();
        }
    }

    private S3Client createS3Client() {
        return S3Client.builder()
                .region(Region.of(config.s3().region()))
                .endpointOverride(URI.create(config.s3().endpoint()))
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(config.s3().accessKey(), config.s3().secretKey())))
                .httpClientBuilder(ApacheHttpClient.builder())
                .build();
    }
}