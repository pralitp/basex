package org.basex.io.random;

import static org.basex.data.DataText.*;

import java.io.*;

import org.basex.data.*;
import org.basex.io.*;
import org.basex.util.Util;

/**
 * This provides read-only access to a TableDiskAccess such that reads may be
 * made in parallel.  Any attempt to write to the underlying table while
 * read-only access is occurring will result in an exception.
 *
 * @author BaseX Team 2005-15, BSD License
 * @author Pralit Patel
 */
class ReadOnlyTableDiskAccess extends TableAccess {
  /** The underlying TableDiskAccess. */
  final TableDiskAccess diskAccess;
  /** Our own instance of TableDiskPageData such that reads can be made concurrently */
  final TableDiskPageData currPageData = new TableDiskPageData();

  /**
   * Package level constructor since we only want TableDiskAccess to be able
   * to create instances of this class.
   * @param diskAccess The underlying TableDiskAccess.
   */
  ReadOnlyTableDiskAccess(final TableDiskAccess diskAccess) {
    super(diskAccess.meta);
    this.diskAccess = diskAccess;
    // initialize data file for read only
    try {
    currPageData.file = new RandomAccessFile(meta.dbfile(DATATBL).file(), "r");
    } catch(IOException e) {
      e.printStackTrace();
      // TODO: what to do with this?
      throw Util.notExpected("Could not open dbfile as read-only");
    }
  }

  @Override
  public void flush(final boolean all) throws IOException {
    // TODO: should not get this?
    System.out.println("======Flush request");
    Thread.dumpStack();
  }

  @Override
  public void close() throws IOException {
    currPageData.file.close();
  }

  @Override
  public boolean lock(final boolean write) {
    // TODO: should not get this?
    System.out.println("======Lock request");
    return false;
  }

  @Override
  public TableAccess getReadOnlyTableAccess() {
    return diskAccess.getReadOnlyTableAccess();
  }

  @Override
  public int read1(int p, int o) {
    return diskAccess.read1(p, o, currPageData);
  }

  @Override
  public int read2(int p, int o) {
    return diskAccess.read2(p, o, currPageData);
  }

  @Override
  public int read4(int p, int o) {
    return diskAccess.read4(p, o, currPageData);
  }

  @Override
  public long read5(int p, int o) {
    return diskAccess.read5(p, o, currPageData);
  }

  @Override
  public void write1(int p, int o, int v) {
    throw Util.notExpected("Can not write to read-only table disk access");
  }

  @Override
  public void write2(int p, int o, int v) {
    throw Util.notExpected("Can not write to read-only table disk access");
  }

  @Override
  public void write4(int p, int o, int v) {
    throw Util.notExpected("Can not write to read-only table disk access");
  }

  @Override
  public void write5(int p, int o, long v) {
    throw Util.notExpected("Can not write to read-only table disk access");
  }

  @Override
  protected void dirty() {
    throw Util.notExpected("Can not write to read-only table disk access");
  }

  @Override
  protected void copy(final byte[] entries, final int pre, final int last) {
    throw Util.notExpected("Can not write to read-only table disk access");
  }

  @Override
  public void delete(int pre, int nr) {
    throw Util.notExpected("Can not write to read-only table disk access");
  }

  @Override
  public void insert(int pre, byte[] entries) {
    throw Util.notExpected("Can not write to read-only table disk access");
  }
}
