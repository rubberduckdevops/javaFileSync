package rocks.michaelhall;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/examples-s3-transfermanager.html
public class RemoteStorage {
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

    public void uploadlocalFilestoS3(Map<String, String> filePaths, String s3BucketName, String s3KeyName) {
        ArrayList<File> files = new ArrayList<>();
        filePaths.forEach((filePath,hash) -> {
            logger.info("File: " + filePath " Upload to s3!");
            files.add(new File((string)filePath));
        });
        TransferManager xfer_mgr = TransferManagerBuilder.standard().build();
        try {
            MultipleFileUpload xfer = xfer_mgr.uploadFileList(s3BucketName, s3KeyName, new File("."), files);
            // loop with Transfer.isDone()
            XferMgrProgress.showTransferProgress(xfer);
            // or block with Transfer.waitForCompletion()
            XferMgrProgress.waitForCompletion(xfer);
        } catch (AmazonServiceExcetpion e) {
            logger.error("Something went wrong uploading {} to s3Bucket {}",filePaths, s3BucketName, e);
        }
        xfer_mgr.shutdownNow();
    }
}
