package rocks.michaelhall;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.Map;

// https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/examples-s3-transfermanager.html
public class RemoteS3Storage {
    protected static final Logger logger = LogManager.getLogger();

    public void uploadlocalFiletoS3(String filePath, String s3BucketName, String s3KeyName) {
        File f = new File(filePath);
        TransferManager xfer_mgr = TransferManagerBuilder.standard().build();
        try {
            Upload xfer = xfer_mgr.upload(s3BucketName, s3KeyName, f);
            // loop with Transfer.isDone()
            XferMgrProgress.showTransferProgress(xfer);
            //  or block with Transfer.waitForCompletion()
            XferMgrProgress.waitForCompletion(xfer);
        } catch (AmazonServiceException e) {
            logger.error("Something went wrong uploading {} to s3Bucket {}",filePath, s3BucketName, e);
            // Should probably exit here!
        }
        xfer_mgr.shutdownNow();
    }
    // Update the method to use https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/PutObjectRequest.html
    public void uploadlocalMultipleFilestoS3(Map<String, String> filePaths, String s3BucketName, String s3KeyName) {
        ArrayList<File> files = new ArrayList<>();
        filePaths.forEach((filePath, hash) -> {
            logger.info("File: " + filePath + " Upload to s3!");
            files.add(new File(filePath));
        });
        TransferManager xfer_mgr = TransferManagerBuilder.standard().build();
        try {
            MultipleFileUpload xfer = xfer_mgr.uploadFileList(s3BucketName, s3KeyName, new File("."), files);
            // loop with Transfer.isDone()
            XferMgrProgress.showTransferProgress(xfer);
            // or block with Transfer.waitForCompletion()
            XferMgrProgress.waitForCompletion(xfer);
        } catch (AmazonServiceException e) {
            logger.error("Something went wrong uploading {} to s3Bucket {}", filePaths, s3BucketName, e);
        }
        xfer_mgr.shutdownNow();
    }
    // One issue we may have is there is no checksum uploaded with some of the files. We need to ensure that the checksum is also uploaded with the file!
    public void listFilesInS3Bucket(String s3BucketName) {
        Regions clientRegion = Regions.DEFAULT_REGION;
        try {
            AmazonS3 s3client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new ProfileCredentialsProvider())
                    .withRegion(clientRegion).build();
            logger.info("Listing files in s3 bucket " + s3BucketName);
            // Adding withMaxKeys(2) to utilize the next token
            ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(s3BucketName).withMaxKeys(2);
            ListObjectsV2Result result;
            do {
                result = s3client.listObjectsV2(request);
                for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                    logger.info(objectSummary.getKey(), objectSummary.getSize());
                }
                String token = result.getNextContinuationToken();
                logger.info("Next Continuation token: " + token);
                request.setContinuationToken(token);
            } while (result.isTruncated());
        } catch (AmazonServiceException e) {
            logger.error("Something went wrong with the s3 service, bucket name: {}", s3BucketName, e);
        } catch (SdkClientException e) {
            logger.error("Something went wrong with the S3 Client, bucket name {}", s3BucketName, e);
        }
    }
}
