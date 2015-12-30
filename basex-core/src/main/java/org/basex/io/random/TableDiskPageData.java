package org.basex.io.random;

import java.io.*;

import org.basex.data.*;
import org.basex.io.*;
import org.basex.util.Util;

/**
 * A simple struct that contains all of the cursors, etc to read from a disk
 * table page by page.
 *
 * @author BaseX Team 2005-15, BSD License
 * @author Pralit Patel
 */
class TableDiskPageData {
  /** Buffer manager. */
  public final Buffers bm = new Buffers();
  /** File storing all pages. */
  RandomAccessFile file;
  /** Pointer to current page. */
  public int page = -1;
  /** Pre value of the first entry in the current page. */
  public int firstPre = -1;
  /** First pre value of the next page. */
  public int nextPre = -1;
  /** Read lock to prevent multiple readers modifying this page */
  public Object lock = new Object();
}
