package rocks.michaelhall;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.FileVisitResult;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import com.google.common.collect.MapDifference;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SyncFileHandler {
    protected static final Logger logger = LogManager.getLogger();

    public Map<String,String> getFilesAndHashes(String dir) throws IOException {
        Map<String, String> fileHashMap = new HashMap<String, String>();
        Files.walkFileTree(Paths.get(dir), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (!Files.isDirectory(file)) {
                    String realFilePath = file.toRealPath().toString();
                    logger.debug("Found file: {}", file.toRealPath().toString());
                    byte[] data = Files.readAllBytes(file);
                    byte[] hash = null;
                    try {
                        hash = MessageDigest.getInstance("MD5").digest(data);
                    } catch (NoSuchAlgorithmException e) {
                        throw new RuntimeException(e);
                    }
                    String checksum = new BigInteger(1, hash).toString(16);
                    logger.debug("File Hash: {}", checksum);
                    fileHashMap.put(realFilePath, checksum);
                }
                return FileVisitResult.CONTINUE;
            }
        });
        logger.info("Finished scanning local files, {} files processed", fileHashMap.size());
        return fileHashMap;
    };

    public Boolean checkLocalCache(String cachePath) {
      if (Files.exists(Paths.get(cachePath))) {
          return Boolean.TRUE;
      } else {
          return Boolean.FALSE;
      }
    };

    public void saveLocalCacheFile(String cachePath, Map<String, String> fileHashMap) throws IOException {
        logger.debug("Saving hash map to cache: {}", cachePath);
        File cacheFileObj = new File(cachePath);
        if (!cacheFileObj.exists()) {
            logger.info("File Does not exist! Creating");
            cacheFileObj.getParentFile().mkdirs();
            cacheFileObj.createNewFile();
        }
        Properties properties = new Properties();
        fileHashMap.forEach(properties::setProperty);

        try (OutputStream out = Files.newOutputStream(Path.of(cachePath))) {
            properties.store(out, "File Hash Cache");
        }
        logger.info("Successfully saved {} entries to cache", fileHashMap.size());
    }

    public Map<String, String> loadLocalCacheFile(String cachePath) throws IOException {
        logger.debug("Loading cache: {}", cachePath);
        Map<String, String> fileHashMap = new HashMap<>();
        if(!Files.exists(Path.of(cachePath))) {
            logger.error("Cache does not exist...");
            return fileHashMap;
        }
        Properties properties = new Properties();
        try(InputStream in = Files.newInputStream(Path.of(cachePath))) {
            properties.load(in);
        }

        properties.forEach((key, value) -> {fileHashMap.put(key.toString(), value.toString());});
        logger.info("Successfully loaded {} entries from cache", fileHashMap.size());
        return fileHashMap;
    }

    public Map<String,ValueDifference<String>> compareLocalCacheToUpdatedCacheExistingFiles(Map<String, String> localCache, Map<String, String> updatedCache) {
        MapDifference<String,String> diff = Maps.difference(localCache, updatedCache);
        logger.info("List of entries that have different Hashes");
        logger.info(diff.entriesDiffering());
        return diff.entriesDiffering();
    }
    public Map<String,String> compareLocalCacheToUpdatedCacheOnLeft(Map<String, String> localCache, Map<String, String> updatedCache) {
        MapDifference<String,String> diff = Maps.difference(localCache, updatedCache);
        logger.info("List of files that are only in Local Cache");
        Map<String,String> onlyOnLeft = diff.entriesOnlyOnLeft();
        logger.info(onlyOnLeft);
        return onlyOnLeft;
    }
    public Map<String,String> compareLocalCacheToUpdatedCacheOnRight(Map<String, String> localCache, Map<String, String> updatedCache) {
        MapDifference<String,String> diff = Maps.difference(localCache, updatedCache);
        logger.info("List of files that are only in Updated Cache");
        Map<String,String> onlyOnRight = diff.entriesOnlyOnRight();
        logger.info(onlyOnRight);
        return onlyOnRight;
    }
}
