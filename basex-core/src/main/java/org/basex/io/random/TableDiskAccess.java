package org.basex.io.random;

import static org.basex.data.DataText.*;

import java.io.*;
import java.nio.channels.*;
import java.util.*;

import org.basex.core.*;
import org.basex.data.*;
import org.basex.io.*;
import org.basex.io.in.DataInput;
import org.basex.io.out.DataOutput;
import org.basex.util.*;

/**
 * This class stores the table on disk and reads it page-wise.
 *
 * NOTE: this class is not thread-safe.
 *
 * @author BaseX Team 2005-15, BSD License
 * @author Christian Gruen
 * @author Tim Petrowsky
 */
public final class TableDiskAccess extends TableAccess {
  /** Bitmap storing free (=0) and used (=1) pages. */
  private BitArray usedPages;
  /** File lock. */
  private FileLock fl;

  /** First pre values (ascending order); will be initialized with the first update. */
  private int[] fpres;
  /** Page index; will be initialized with the first update. */
  private int[] pages;
  /** Total number of pages. */
  private int size;
  /** Number of used pages. */
  private int used;

  class PageData {
    /** Buffer manager. */
    public final Buffers bm = new Buffers();
    /** File storing all pages. */
    private RandomAccessFile file;
    /** Pointer to current page. */
    public int page = -1;
    /** Pre value of the first entry in the current page. */
    public int firstPre = -1;
    /** First pre value of the next page. */
    public int nextPre = -1;
    /** Read lock to prevent multiple readers modifying this page */
    public Object lock = new Object();
  };

  private final PageData currPageData = new PageData();

  /**
   * Constructor.
   * @param md meta data
   * @param write write lock
   * @throws IOException I/O exception
   */
  public TableDiskAccess(final MetaData md, final boolean write) throws IOException {
    super(md);

    // read meta and index data
    try(final DataInput in = new DataInput(meta.dbfile(DATATBL + 'i'))) {
      final int s = in.readNum();
      size = s;

      // check if page index is regular and can be calculated (0: no pages)
      final int u = in.readNum();
      final boolean regular = u == 0 || u == Integer.MAX_VALUE;
      if(regular) {
        used = u == 0 ? 0 : s;
      } else {
        // read page index and first pre values from disk
        used = u;
        fpres = in.readNums();
        pages = in.readNums();
      }

      // read block bitmap
      if(!regular) {
        final int psize = in.readNum();
        usedPages = new BitArray(in.readLongs(psize), used);
      }
    }

    // initialize data file
    currPageData.file = new RandomAccessFile(meta.dbfile(DATATBL).file(), "rw");
    if(!lock(write)) throw new BaseXException(Text.DB_PINNED_X, md.name);
  }

  /**
   * Checks if the table of the specified database is locked.
   * @param db name of database
   * @param ctx database context
   * @return result of check
   */
  public static boolean locked(final String db, final Context ctx) {
    final IOFile table = MetaData.file(ctx.soptions.dbpath(db), DATATBL);
    if(!table.exists()) return false;

    try(final RandomAccessFile file = new RandomAccessFile(table.file(), "rw")) {
      return file.getChannel().tryLock() == null;
    } catch(final IOException ex) {
      return true;
    }
  }

  @Override
  public void flush(final boolean all) throws IOException {
    synchronized(currPageData.lock) {
      for(final Buffer b : currPageData.bm.all()) if(b.dirty) write(b);
      if(!dirty || !all) return;

      try(final DataOutput out = new DataOutput(meta.dbfile(DATATBL + 'i'))) {
        final int sz = size;
        out.writeNum(sz);
        out.writeNum(used);
        // due to legacy issues, number of pages is written several times
        out.writeNum(sz);
        for(int s = 0; s < sz; s++) out.writeNum(fpres[s]);
        out.writeNum(sz);
        for(int s = 0; s < sz; s++) out.writeNum(pages[s]);

        out.writeLongs(usedPages.toArray());
      }
      dirty = false;
    }
  }

  @Override
  public void close() throws IOException {
    synchronized(currPageData.lock) {
      flush(true);
      currPageData.file.close();
    }
  }

  @Override
  public boolean lock(final boolean write) {
    try {
      if(fl != null) {
        if(write != fl.isShared()) return true;
        fl.release();
      }
      fl = currPageData.file.getChannel().tryLock(0, Long.MAX_VALUE, !write);
      return fl != null;
    } catch(final IOException ex) {
      throw Util.notExpected(ex);
    }
  }

  @Override
  public int read1(final int pre, final int off) {
      return read1(pre, off, currPageData);
  }
  int read1(final int pre, final int off, PageData pageData) {
    synchronized(pageData.lock) {
      final int o = off + cursor(pre, pageData);
      final byte[] b = pageData.bm.current().data;
      return b[o] & 0xFF;
    }
  }


  @Override
  public int read2(final int pre, final int off) {
      return read2(pre, off, currPageData);
  }
  int read2(final int pre, final int off, PageData pageData) {
    synchronized(pageData.lock) {
      final int o = off + cursor(pre, pageData);
      final byte[] b = pageData.bm.current().data;
      return ((b[o] & 0xFF) << 8) + (b[o + 1] & 0xFF);
    }
  }

  @Override
  public int read4(final int pre, final int off) {
      return read4(pre, off, currPageData);
  }
  int read4(final int pre, final int off, PageData pageData) {
    synchronized(pageData.lock) {
      final int o = off + cursor(pre, pageData);
      final byte[] b = pageData.bm.current().data;
      return ((b[o] & 0xFF) << 24) + ((b[o + 1] & 0xFF) << 16) +
        ((b[o + 2] & 0xFF) << 8) + (b[o + 3] & 0xFF);
    }
  }

  @Override
  public long read5(final int pre, final int off) {
      return read5(pre, off, currPageData);
  }
  long read5(final int pre, final int off, PageData pageData) {
    synchronized(pageData.lock) {
      final int o = off + cursor(pre, pageData);
      final byte[] b = pageData.bm.current().data;
      return ((long) (b[o] & 0xFF) << 32) + ((long) (b[o + 1] & 0xFF) << 24) +
        ((b[o + 2] & 0xFF) << 16) + ((b[o + 3] & 0xFF) << 8) + (b[o + 4] & 0xFF);
    }
  }

  @Override
  public void write1(final int pre, final int off, final int v) {
    final int o = off + cursor(pre, currPageData);
    final Buffer bf = currPageData.bm.current();
    final byte[] b = bf.data;
    b[o] = (byte) v;
    bf.dirty = true;
  }

  @Override
  public void write2(final int pre, final int off, final int v) {
    final int o = off + cursor(pre, currPageData);
    final Buffer bf = currPageData.bm.current();
    final byte[] b = bf.data;
    b[o] = (byte) (v >>> 8);
    b[o + 1] = (byte) v;
    bf.dirty = true;
  }

  @Override
  public void write4(final int pre, final int off, final int v) {
    final int o = off + cursor(pre, currPageData);
    final Buffer bf = currPageData.bm.current();
    final byte[] b = bf.data;
    b[o]     = (byte) (v >>> 24);
    b[o + 1] = (byte) (v >>> 16);
    b[o + 2] = (byte) (v >>> 8);
    b[o + 3] = (byte) v;
    bf.dirty = true;
  }

  @Override
  public void write5(final int pre, final int off, final long v) {
    final int o = off + cursor(pre, currPageData);
    final Buffer bf = currPageData.bm.current();
    final byte[] b = bf.data;
    b[o]     = (byte) (v >>> 32);
    b[o + 1] = (byte) (v >>> 24);
    b[o + 2] = (byte) (v >>> 16);
    b[o + 3] = (byte) (v >>> 8);
    b[o + 4] = (byte) v;
    bf.dirty = true;
  }

  @Override
  protected void copy(final byte[] entries, final int pre, final int last) {
    for(int o = 0, i = pre; i < last; ++i, o += IO.NODESIZE) {
      final int off = cursor(i, currPageData);
      final Buffer bf = currPageData.bm.current();
      System.arraycopy(entries, o, bf.data, off, IO.NODESIZE);
      bf.dirty = true;
    }
  }

  @Override
  public void delete(final int pre, final int nr) {
    if(nr == 0) return;

    // get first page
    dirty();
    cursor(pre, currPageData);

    // some useful variables to make code more readable
    int from = pre - currPageData.firstPre;
    final int last = pre + nr;

    // check if all entries are in current page: handle and return
    if(last - 1 < currPageData.nextPre) {
      final Buffer bf = currPageData.bm.current();
      copy(bf.data, from + nr, bf.data, from, currPageData.nextPre - last);
      updatePre(nr);

      // if whole page was deleted, remove it from the index
      if(currPageData.nextPre == currPageData.firstPre) {
        // mark the page as empty
        usedPages.clear(pages[currPageData.page]);

        Array.move(fpres, currPageData.page + 1, -1, used - currPageData.page - 1);
        Array.move(pages, currPageData.page + 1, -1, used - currPageData.page - 1);

        --used;
        readPage(currPageData.page, currPageData);
      }
      return;
    }

    // handle pages whose entries are to be deleted entirely

    // first count them
    int unused = 0;
    while(currPageData.nextPre < last) {
      if(from == 0) {
        ++unused;
        // mark the pages as empty; range clear cannot be used because the
        // pages may not be consecutive
        usedPages.clear(pages[currPageData.page]);
      }
      setPage(currPageData.page + 1, currPageData);
      from = 0;
    }

    // if the last page is empty, clear the corresponding bit
    read(pages[currPageData.page], currPageData);
    final Buffer bf = currPageData.bm.current();
    if(currPageData.nextPre == last) {
      usedPages.clear((int) bf.pos);
      ++unused;
      if(currPageData.page < used - 1) readPage(currPageData.page + 1, currPageData);
      else ++currPageData.page;
    } else {
      // delete entries at beginning of current (last) page
      copy(bf.data, last - currPageData.firstPre, bf.data, 0, currPageData.nextPre - last);
    }

    // now remove them from the index
    if(unused > 0) {
      Array.move(fpres, currPageData.page, -unused, used - currPageData.page);
      Array.move(pages, currPageData.page, -unused, used - currPageData.page);
      used -= unused;
      currPageData.page -= unused;
    }

    // update index entry for this page
    fpres[currPageData.page] = pre;
    currPageData.firstPre = pre;
    updatePre(nr);
  }

  @Override
  public void insert(final int pre, final byte[] entries) {
    final int nnew = entries.length;
    if(nnew == 0) return;
    dirty();

    // number of records to be inserted
    final int nr = nnew >>> IO.NODEPOWER;

    int split = 0;
    if(used == 0) {
      // special case: insert new data into first page if database is empty
      readPage(0, currPageData);
      usedPages.set(0);
      ++used;
    } else if(pre > 0) {
      // find the offset within the page where the new records will be inserted
      split = cursor(pre - 1, currPageData) + IO.NODESIZE;
    }

    // number of bytes occupied by old records in the current page
    final int nold = currPageData.nextPre - currPageData.firstPre << IO.NODEPOWER;
    // number of bytes occupied by old records which will be moved at the end
    final int moved = nold - split;

    // special case: all entries fit in the current page
    Buffer bf = currPageData.bm.current();
    if(nold + nnew <= IO.BLOCKSIZE) {
      Array.move(bf.data, split, nnew, moved);
      System.arraycopy(entries, 0, bf.data, split, nnew);
      bf.dirty = true;

      // increment first pre-values of pages after the last modified page
      for(int i = currPageData.page + 1; i < used; ++i) fpres[i] += nr;
      // update cached variables (fpre is not changed)
      currPageData.nextPre += nr;
      meta.size += nr;
      return;
    }

    // append old entries at the end of the new entries
    final byte[] all = new byte[nnew + moved];
    System.arraycopy(entries, 0, all, 0, nnew);
    System.arraycopy(bf.data, split, all, nnew, moved);

    // fill in the current page with new entries
    // number of bytes which fit in the first page
    int nrem = IO.BLOCKSIZE - split;
    if(nrem > 0) {
      System.arraycopy(all, 0, bf.data, split, nrem);
      bf.dirty = true;
    }

    // number of new required pages and remaining bytes
    final int req = all.length - nrem;
    int needed = req / IO.BLOCKSIZE;
    final int remain = req % IO.BLOCKSIZE;

    if(remain > 0) {
      // check if the last entries can fit in the page after the current one
      if(currPageData.page + 1 < used) {
        final int o = occSpace(currPageData.page + 1) << IO.NODEPOWER;
        if(remain <= IO.BLOCKSIZE - o) {
          // copy the last records
          readPage(currPageData.page + 1, currPageData);
          bf = currPageData.bm.current();
          System.arraycopy(bf.data, 0, bf.data, remain, o);
          System.arraycopy(all, all.length - remain, bf.data, 0, remain);
          bf.dirty = true;
          // reduce the pre value, since it will be later incremented with nr
          fpres[currPageData.page] -= remain >>> IO.NODEPOWER;
          // go back to the previous page
          readPage(currPageData.page - 1, currPageData);
        } else {
          // there is not enough space in the page - allocate a new one
          ++needed;
        }
      } else {
        // this is the last page - allocate a new one
        ++needed;
      }
    }

    // number of expected pages: existing pages + needed page - empty pages
    final int exp = size + needed - (size - used);
    if(exp > fpres.length) {
      // resize directory arrays if existing ones are too small
      final int ns = Math.max(fpres.length << 1, exp);
      fpres = Arrays.copyOf(fpres, ns);
      pages = Arrays.copyOf(pages, ns);
    }

    // make place for the pages where the new entries will be written
    Array.move(fpres, currPageData.page + 1, needed, used - currPageData.page - 1);
    Array.move(pages, currPageData.page + 1, needed, used - currPageData.page - 1);

    // write the all remaining entries
    while(needed-- > 0) {
      freePage();
      nrem += write(all, nrem);
      fpres[currPageData.page] = fpres[currPageData.page - 1] + IO.ENTRIES;
      pages[currPageData.page] = (int) currPageData.bm.current().pos;
    }

    // increment all fpre values after the last modified page
    for(int i = currPageData.page + 1; i < used; ++i) fpres[i] += nr;

    meta.size += nr;

    // update cached variables
    currPageData.firstPre = fpres[currPageData.page];
    currPageData.nextPre = currPageData.page + 1 < used && fpres[currPageData.page + 1] < meta.size ? fpres[currPageData.page + 1] : meta.size;
  }

  @Override
  protected void dirty() {
    // initialize data structures required for performing updates
    if(fpres == null) {
      final int b = size;
      fpres = new int[b];
      pages = new int[b];
      for(int i = 0; i < b; i++) {
        fpres[i] = i * IO.ENTRIES;
        pages[i] = i;
      }
      usedPages = new BitArray(used, true);
    }
    dirty = true;
  }

  // PRIVATE METHODS ==========================================================

  /**
   * Searches for the page containing the entry for the specified pre value.
   * Reads the page and returns its offset inside the page.
   * @param pre pre of the entry to search for
   * @return offset of the entry in the page
   */
  private int cursor(final int pre, PageData pageData) {
    // TOOD: is this synchronized really necessary?
    synchronized(pageData.lock) {
      int fp = pageData.firstPre, np = pageData.nextPre;
      if(pre < fp || pre >= np) {
        final int last = used - 1;
        int l = 0, h = last, m = pageData.page;
        while(l <= h) {
          if(pre < fp) h = m - 1;
          else if(pre >= np) l = m + 1;
          else break;
          m = h + l >>> 1;
          fp = fpre(m);
          np = m == last ? meta.size : fpre(m + 1);
        }
        if(l > h) throw Util.notExpected(
            "Data Access out of bounds:" +
            "\n- pre value: " + pre +
            "\n- table size: " + meta.size +
            "\n- first/next pre value: " + fp + '/' + np +
            "\n- #total/used pages: " + size + '/' + used +
            "\n- accessed page: " + m + " (" + l + " > " + h + ']');
        readPage(m, pageData);
      }
      return pre - pageData.firstPre << IO.NODEPOWER;
    }
  }

  /**
   * Updates the page pointers.
   * @param p page index
   */
  private void setPage(final int p, PageData pageData) {
    synchronized(pageData.lock) {
      pageData.page = p;
      pageData.firstPre = fpre(p);
      pageData.nextPre = p + 1 >= used ? meta.size : fpre(p + 1);
    }
  }

  /**
   * Updates the index pointers and fetches the requested page.
   * @param p page index
   */
  private void readPage(final int p, PageData pageData) {
    synchronized(pageData.lock) {
      setPage(p, pageData);
      read(page(p), pageData);
    }
  }

  /**
   * Return the specified page index.
   * @param p index of the page to fetch
   * @return pre value
   */
  private int page(final int p) {
    // TODO: is the synchronized necessary?
    //synchronized(currPageData.lock) {
      return pages == null ? p : pages[p];
    //}
  }

  /**
   * Return the specified pre value.
   * @param p index of the page to fetch
   * @return pre value
   */
  private int fpre(final int p) {
    // TODO: is the synchronized necessary?
    //synchronized(currPageData.lock) {
      return fpres == null ? p * IO.ENTRIES : fpres[p];
    //}
  }

  /**
   * Reads a page from disk.
   * @param p page to fetch
   */
  private void read(final int p, PageData pageData) {
    // TODO: is this synchronized really necessary
    synchronized(pageData.lock) {
      if(!pageData.bm.cursor(p)) return;

      final Buffer bf = pageData.bm.current();
      try {
        if(bf.dirty) {
          if(pageData != currPageData) {
            throw Util.notExpected("Dirty buffer from read-only TableDiskAccess call");
          }
          write(bf);
        }
        bf.pos = p;
        if(p >= size) {
          size = p + 1;
        } else {
          pageData.file.seek(bf.pos * IO.BLOCKSIZE);
          pageData.file.readFully(bf.data);
        }
      } catch(final IOException ex) {
        Util.stack(ex);
      }
    }
  }

  /**
   * Moves the cursor to a free page (either new or existing empty one).
   */
  private void freePage() {
    final int p = usedPages.nextFree(0);
    usedPages.set(p);
    read(p, currPageData);
    ++used;
    ++currPageData.page;
  }

  /**
   * Writes the specified buffer disk and resets the dirty flag.
   * @param bf buffer to write
   * @throws IOException I/O exception
   */
  private void write(final Buffer bf) throws IOException {
    currPageData.file.seek(bf.pos * IO.BLOCKSIZE);
    currPageData.file.write(bf.data);
    bf.dirty = false;
  }

  /**
   * Updates the firstPre index entries.
   * @param nr number of entries to move
   */
  private void updatePre(final int nr) {
    // update index entries for all following pages and reduce counter
    for(int i = currPageData.page + 1; i < used; ++i) fpres[i] -= nr;
    meta.size -= nr;
    currPageData.nextPre = currPageData.page + 1 < used && fpres[currPageData.page + 1] < meta.size ? fpres[currPageData.page + 1] : meta.size;
  }

  /**
   * Convenience method for copying pages.
   * @param s source array
   * @param sp source position
   * @param d destination array
   * @param dp destination position
   * @param l source length
   */
  private void copy(final byte[] s, final int sp, final byte[] d, final int dp, final int l) {
    System.arraycopy(s, sp << IO.NODEPOWER, d, dp << IO.NODEPOWER, l << IO.NODEPOWER);
    currPageData.bm.current().dirty = true;
  }

  /**
   * Fill the current buffer with bytes from the specified array from the
   * specified offset.
   * @param s source array
   * @param o offset from the beginning of the array
   * @return number of written bytes
   */
  private int write(final byte[] s, final int o) {
    final Buffer bf = currPageData.bm.current();
    final int len = Math.min(IO.BLOCKSIZE, s.length - o);
    System.arraycopy(s, o, bf.data, 0, len);
    bf.dirty = true;
    return len;
  }

  /**
   * Calculate the occupied space in a page.
   * @param i page index
   * @return occupied space in number of records
   */
  private int occSpace(final int i) {
    return (i + 1 < used ? fpres[i + 1] : meta.size) - fpres[i];
  }
}
