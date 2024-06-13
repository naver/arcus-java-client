package net.spy.memcached.transcoders;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import net.spy.memcached.compat.CloseUtil;
import net.spy.memcached.compat.SpyObject;

public class GzipCompressor extends SpyObject implements Compressor {
  private int compressionThreshold;

  public GzipCompressor() {
    this.compressionThreshold = DEFAULT_COMPRESSION_THRESHOLD;
  }

  public GzipCompressor(int compressionThreshold) {
    this.compressionThreshold = compressionThreshold;
  }

  @Override
  public byte[] compress(byte[] in) {
    if (in == null) {
      throw new NullPointerException("Can't compress null");
    }
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    GZIPOutputStream gz = null;
    try {
      gz = new GZIPOutputStream(bos);
      gz.write(in);
    } catch (IOException e) {
      throw new RuntimeException("IO exception compressing data", e);
    } finally {
      CloseUtil.close(gz);
      CloseUtil.close(bos);
    }
    return bos.toByteArray();
  }

  @Override
  public byte[] decompress(byte[] in) {
    ByteArrayOutputStream bos = null;
    if (in != null) {
      ByteArrayInputStream bis = new ByteArrayInputStream(in);
      bos = new ByteArrayOutputStream();
      GZIPInputStream gis;
      try {
        gis = new GZIPInputStream(bis);
        byte[] buf = new byte[8192];
        int r = -1;
        while ((r = gis.read(buf)) > 0) {
          bos.write(buf, 0, r);
        }
      } catch (IOException e) {
        getLogger().warn("Failed to decompress data", e);
        bos = null;
      }
    }
    return bos == null ? null : bos.toByteArray();
  }

  @Override
  public int getCompressionThreshold() {
    return compressionThreshold;
  }

  @Override
  public void setCompressionThreshold(int compressionThreshold) {
    this.compressionThreshold = compressionThreshold;
  }

}
