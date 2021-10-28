package nl.saccharum.xrpl.neo4j.indexer;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author smelis
 * TODO: Split files up into directories, with batches of configurable size, cause
 * this will get unusable quickly.
 */
public class Cacher {

    private final Path cachePath;

    public Cacher(Path cachePath) {
        this.cachePath = cachePath;
    }

    public File getCachedFile(long ledgerIndex) {
        return Paths.get(cachePath.toString(), ledgerIndex + ".json").toFile();
    }

    public boolean isCached(long ledgerIndex) {
        return getCachedFile(ledgerIndex).exists();
    }

    public String getCachedLedger(long ledgerIndex) throws IOException {
        if (isCached(ledgerIndex)) {
            return FileUtils.readFileToString(getCachedFile(ledgerIndex), Charset.defaultCharset());
        }
        return null;
    }

    public void cacheLedger(String ledger, long ledgerIndex) throws IOException {
        File cacheFile = getCachedFile(ledgerIndex);
        if (!cacheFile.exists()) {
            FileUtils.writeStringToFile(cacheFile, ledger, Charset.defaultCharset());
        }
    }

}
