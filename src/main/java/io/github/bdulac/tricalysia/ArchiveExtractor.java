package io.github.bdulac.tricalysia;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ArchiveExtractor implements TriplesExtractor {

	@Override
	public List<String> getSupportedMimeTypes() {
		List<String> result = new ArrayList<String>();
		result.add("application/zip");
		return result;
	}

	@Override
	public List<URL> extract(Tricalysia store, URL url) throws IOException {
        byte[] buffer = new byte[1024];
        File file = File.createTempFile("" + url.hashCode(), ".zip");
        ReadableByteChannel chan = Channels.newChannel(url.openStream());
        FileOutputStream copyStream = new FileOutputStream(file);
        copyStream.getChannel().transferFrom(chan, 0, Long.MAX_VALUE);
        copyStream.close();
        List<URL> result = new ArrayList<URL>();
        ZipInputStream zis = new ZipInputStream(new FileInputStream(file));
        ZipEntry zipEntry = zis.getNextEntry();
        while(zipEntry != null){
            String fileName = zipEntry.getName();
            int suffixIndex = fileName.lastIndexOf(".");
            String suffix = null;
            if(suffixIndex >= 0) {
            	suffix = fileName.substring(suffixIndex);
            	fileName = fileName.substring(0, suffixIndex);
            }
            File newFile = File.createTempFile(fileName, suffix);
            FileOutputStream unzipStream = new FileOutputStream(newFile);
            int len;
            while ((len = zis.read(buffer)) > 0) {
                unzipStream.write(buffer, 0, len);
            }
            unzipStream.close();
            zipEntry = zis.getNextEntry();
            URL u = newFile.toURI().toURL();
            result.add(u);
            newFile.deleteOnExit();
        }
        zis.closeEntry();
        zis.close();
        file.deleteOnExit();
		return result;
	}

}
