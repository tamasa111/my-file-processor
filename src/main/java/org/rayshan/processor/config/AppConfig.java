package org.rayshan.processor.config;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "app.config")
public interface AppConfig {
    S3Config s3();
    KafkaConfig kafka();

    interface KafkaConfig {
        String offsetReset();
        String outputTopic();
        String outputTopicPrefix();
    }

    interface S3Config {
        String endpoint();
        String bucketName();
        String region();
        String accessKey();
        String secretKey();
    }
}
