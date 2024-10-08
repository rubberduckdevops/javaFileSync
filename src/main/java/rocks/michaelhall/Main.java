package rocks.michaelhall;

import com.google.common.collect.MapDifference.ValueDifference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;

public class Main {
    protected static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) throws IOException {
        logger.info("Starting Application");
        String dirToSync = "C:\\Users\\michael\\Documents\\obsidian\\Personal";
        String syncCacheFile = "C:\\Users\\michael\\.javaFileSync\\cache";
        SyncFileHandler fh = new SyncFileHandler();
        try {
            logger.info("Getting local files and hashes.");

            Map<String,String> syncFiles = fh.getFilesAndHashes(dirToSync);
            syncFiles.forEach((path,hash) -> {
                logger.info("File: " + path + " Hash: " + hash);
            });
            logger.info("Checking if local cache exists");
            Boolean localCacheFileExists = fh.checkLocalCache(syncCacheFile);
            logger.info("Local Cache File status: " + localCacheFileExists.toString());
            if (localCacheFileExists) {
                logger.debug("File exists!!!");
                Map<String,String> currentCacheFile = fh.loadLocalCacheFile(syncCacheFile);
                logger.info("Are Cache Maps equal? " + currentCacheFile.equals(syncFiles));
                if (!currentCacheFile.equals(syncFiles)) {
                    //Check for updated files locally
                    Map<String,ValueDifference<String>> newFiles = fh.compareLocalCacheToUpdatedCacheExistingFiles(currentCacheFile, syncFiles);
                    logger.info("Files updated since last sync: {}", newFiles.size());
                    //Check for Deleted Files locally
                    Map<String,String> filesOnlyInLocalCache = fh.compareLocalCacheToUpdatedCacheOnLeft(currentCacheFile, syncFiles);
                    logger.info("Files deleted since last sync: {}", filesOnlyInLocalCache.size());
                    //Check for NEW files locally
                    Map<String,String> filesOnlyInNewCache = fh.compareLocalCacheToUpdatedCacheOnRight(currentCacheFile, syncFiles);
                    logger.info("Files created since last sync: {}", filesOnlyInNewCache.size());
                }
                // Now we need to check S3 for files and a config

            } else {
                logger.debug("Cache does not exist, will initialize it!");
                fh.saveLocalCacheFile(syncCacheFile, syncFiles);
            }

        } catch (IOException e) {
            logger.error("Unable to access the directory or file!", e);

        }

    }
}