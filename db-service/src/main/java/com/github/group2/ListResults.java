package com.github.group2;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;

import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

public class ListResults {
    public static void main(String[] args) {
        try (InputStream input =
                ListResults.class.getClassLoader().getResourceAsStream("app.properties")) {
            Properties prop = new Properties(System.getProperties());
            prop.load(input);
            System.setProperties(prop);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        Region region = Region.US_WEST_1;

        S3Client S3 = S3Client.builder().region(region)
                .credentialsProvider(SystemPropertyCredentialsProvider.create()).build();

        String bucketName = "revature-200413-project2-group2";
        String prefix = "JeffsResults/";
        List<String> keyNames = new ArrayList<>();
        try {
            ListObjectsRequest listObjects =
                    ListObjectsRequest.builder().bucket(bucketName).prefix(prefix).build();

            ListObjectsResponse res = S3.listObjects(listObjects);
            List<S3Object> objects = res.contents();
            System.out.println("Files listed under" + prefix);
            for (ListIterator<S3Object> iter = objects.listIterator(); iter.hasNext();) {
                S3Object myValue = (S3Object) iter.next();
                keyNames.add(myValue.key());
                System.out.println("\nKey name: " + myValue.key());
                System.out.println("Key owner: " + myValue.owner());
            }

            for (String keyName : keyNames) {
                // String req = "/" + bucketName + "/" + prefix + keyName;
                S3.getObject(GetObjectRequest.builder().bucket(bucketName).key(keyName).build(),
                        ResponseTransformer.toFile(new File(keyName.replace(prefix, ""))));
            }
        } catch (S3Exception e) {
            e.printStackTrace();
        }
    }
}
