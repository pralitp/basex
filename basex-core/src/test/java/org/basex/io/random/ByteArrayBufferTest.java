package org.basex.io.random;

public class ByteArrayBufferTest extends BufferTest {
  @Override
  protected Buffer getBuffer(int size) {
    return new ByteArrayBuffer(size);
  }
}
