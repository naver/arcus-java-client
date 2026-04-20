package net.spy.memcached.transcoders.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import net.spy.memcached.compat.SpyObject;


public class GZIPCompressionCodec extends SpyObject implements CompressionCodec {

  private static final int DECOMPRESSION_BUF_SIZE = 8192;

  private volatile int compressionThreshold = 16_384;

  @Override
  public byte[] compress(byte[] in) {
    if (in == null) {
      throw new IllegalArgumentException("Can't compress null");
    }

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (GZIPOutputStream gz = new GZIPOutputStream(bos)) {
      gz.write(in);
    } catch (IOException e) {
      throw new RuntimeException("IO exception compressing data", e);
    }

    byte[] result = bos.toByteArray();
    getLogger().debug("Compressed %d bytes to %d", in.length, result.length);
    return result;
  }

  @Override
  public byte[] decompress(byte[] in) {
    if (in == null) {
      return null;
    }

    try (ByteArrayInputStream bis = new ByteArrayInputStream(in);
         GZIPInputStream gis = new GZIPInputStream(bis);
         ByteArrayOutputStream bos = new ByteArrayOutputStream()) {

      byte[] buf = new byte[DECOMPRESSION_BUF_SIZE];
      int read;
      while ((read = gis.read(buf)) > 0) {
        bos.write(buf, 0, read);
      }

      return bos.toByteArray();
    } catch (IOException e) {
      getLogger().warn("Failed to decompress data", e);
      return null;
    }
  }

  @Override
  public boolean isCompressionCandidate(byte[] data) {
    return data != null && data.length > compressionThreshold;
  }

  @Override
  public void setCompressionThreshold(int threshold) {
    this.compressionThreshold = threshold;
  }

}
