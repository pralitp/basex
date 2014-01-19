package org.basex.io.random;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.basex.io.IOFile;

/**
 * Block-wise file access.
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Dimitar Popov
 */
abstract class BlockFileAccess {

  /**
   * Open a file and create a new {@link BlockFileAccess}.
   * @param fl file to open
   * @return block-wise file access
   * @throws FileNotFoundException if given file does not exist
   */
  public static BlockFileAccess open(IOFile fl) throws FileNotFoundException {
    RandomAccessFile file = new RandomAccessFile(fl.file(), "rw");
    return new RandomAccessFileBlockFileAccess(file);
  }

  /**
   * Close the file handle.
   * @throws IOException I/O exception
   */
  public abstract void close() throws IOException;

  /**
   * Write the given buffer to the file.
   * @param b buffer to write
   * @throws IOException I/O exception
   */
  public abstract void write(Buffer b) throws IOException;

  /**
   * Read a buffer from the file.
   * @param b buffer to read
   * @param max maximal bytes to read
   * @throws IOException I/O exception
   */
  public abstract void read(Buffer b, int max) throws IOException;

  /**
   * Read a buffer from the file.
   * @param b buffer to read
   * @throws IOException I/O exception
   */
  public abstract void read(Buffer b) throws IOException;

  /**
   * Get the length of the file.
   * @return length of the file in bytes
   * @throws IOException I/O exception
   */
  public abstract long length() throws IOException;

  /**
   * Set the new length of the file.
   * @param l new length to set
   * @throws IOException I/O exception
   */
  public abstract void setLength(long l) throws IOException;

  /**
   * Get the underlying file channel.
   * @return file channel
   */
  public abstract FileChannel getChannel();
}
