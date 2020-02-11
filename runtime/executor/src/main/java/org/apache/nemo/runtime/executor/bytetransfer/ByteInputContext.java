/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.executor.bytetransfer;

import io.netty.buffer.ByteBuf;
import org.apache.nemo.runtime.executor.data.MemoryChunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

/**
 * Container for multiple input streams. Represents a transfer context on receiver-side.
 *
 * <h3>Thread safety:</h3>
 * <p>Methods with default access modifier, namely {@link #onNewStream()}, {@link #onByteBuf(ByteBuf)},
 * {@link #onContextClose()}, are not thread-safe, since they are called by a single Netty event loop.</p>
 * <p>Public methods are thread safe,
 * although the execution order may not be linearized if they were called from different threads.</p>
 */
public final class ByteInputContext extends ByteTransferContext {

  private static final Logger LOG = LoggerFactory.getLogger(ByteInputContext.class.getName());

  private final CompletableFuture<Iterator<InputStream>> completedFuture = new CompletableFuture<>();
  private final ClosableBlockingQueue<ByteBufInputStream> byteBufInputStreams = new ClosableBlockingQueue<>();
  private final AtomicReference<ByteBufInputStream> currentByteBufInputStream = new AtomicReference<>();

  private final Iterator<InputStream> inputStreams = new Iterator<InputStream>() {
    @Override
    public boolean hasNext() {
      try {
        return byteBufInputStreams.peek() != null;
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    @Override
    public InputStream next() {
      try {
        return byteBufInputStreams.take();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.error("Interrupted while taking byte buf.", e);
        throw new NoSuchElementException();
      }
    }
  };

  /**
   * Creates an input context.
   *
   * @param remoteExecutorId  id of the remote executor
   * @param contextId         identifier for this context
   * @param contextDescriptor user-provided context descriptor
   * @param contextManager    {@link ContextManager} for the channel
   */
  ByteInputContext(final String remoteExecutorId,
                   final ContextId contextId,
                   final byte[] contextDescriptor,
                   final ContextManager contextManager) {
    super(remoteExecutorId, contextId, contextDescriptor, contextManager);
  }

  /**
   * Returns {@link Iterator} of {@link InputStream}s.
   * This method always returns the same {@link Iterator} instance.
   *
   * @return {@link Iterator} of {@link InputStream}s.
   */
  public Iterator<InputStream> getInputStreams() {
    return inputStreams;
  }

  /**
   * Returns a future, which is completed when the corresponding transfer for this context gets done.
   *
   * @return a {@link CompletableFuture} for the same value that {@link #getInputStreams()} returns
   */
  public CompletableFuture<Iterator<InputStream>> getCompletedFuture() {
    return completedFuture;
  }

  /**
   * Called when a punctuation for sub-stream incarnation is detected.
   */
  void onNewStream() {
    if (currentByteBufInputStream.get() != null) {
      currentByteBufInputStream.get().byteBufQueue.close();
    }
    currentByteBufInputStream.set(new ByteBufInputStream());
    byteBufInputStreams.put(currentByteBufInputStream.get());
  }

  /**
   * Called when {@link ByteBuf} is supplied to this context.
   *
   * @param byteBuf the {@link ByteBuf} to supply
   */
  void onByteBuf(final ByteBuf byteBuf) {
    if (currentByteBufInputStream.get() == null) {
      throw new RuntimeException("Cannot accept ByteBuf: No sub-stream is opened.");
    }
    if (byteBuf.readableBytes() > 0) {
      currentByteBufInputStream.get().byteBufQueue.put(byteBuf);
    } else {
      // ignore empty data frames
      byteBuf.release();
    }
  }

  /**
   * Called when {@link #onByteBuf(ByteBuf)} event is no longer expected.
   */
  void onContextClose() {
    if (currentByteBufInputStream.get() != null) {
      currentByteBufInputStream.get().byteBufQueue.close();
    }
    byteBufInputStreams.close();
    completedFuture.complete(inputStreams);
    deregister();
  }

  @Override
  public void onChannelError(@Nullable final Throwable cause) {
    setChannelError(cause);

    if (currentByteBufInputStream.get() != null) {
      currentByteBufInputStream.get().byteBufQueue.closeExceptionally(cause);
    }
    byteBufInputStreams.closeExceptionally(cause);
    completedFuture.completeExceptionally(cause);
    deregister();
  }

  /**
   * An {@link InputStream} implementation that reads data from a composition of {@link ByteBuf}s.
   */
  public static final class ByteBufInputStream extends InputStream {

    private final ClosableBlockingQueue<ByteBuf> byteBufQueue = new ClosableBlockingQueue<>();

    private Iterator<ByteBuffer> byteBufferIter = null;
    private ByteBuffer currentByteBuffer = null;
    private long currentByteBufferAddress = -1L;

    @Override
    public int read() throws IOException {
      try {
        final ByteBuf head = byteBufQueue.peek();
        if (head == null) {
          // end of stream event
          return -1;
        }
        final int b = head.readUnsignedByte();
        if (head.readableBytes() == 0) {
          // remove and release header if no longer required
          byteBufQueue.take();
          head.release();
        }
        return b;
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }

    public long readLongAddress() throws IOException {
      try {
        if (currentByteBuffer == null || currentByteBuffer.remaining() <= 0) {
          if (byteBufferIter == null || !byteBufferIter.hasNext()) {
            final ByteBuf nextByteBuf = byteBufQueue.peek();
            if (nextByteBuf == null) {
              return -1;
            }
            byteBufferIter = Arrays.asList(nextByteBuf.nioBuffers()).iterator();
            byteBufQueue.take();
          }
          currentByteBuffer = byteBufferIter.next();
          currentByteBufferAddress = MemoryChunk.getAddress(currentByteBuffer);
        }
        if (currentByteBuffer.remaining() < 9) {
          // This means that integer value is fragmented inside byteBuf!
          // We do not support this situation right now...
          throw new RuntimeException("Found fragmented off-heap nemo event!");
        } else {
          final int oldPos = currentByteBuffer.position();
          currentByteBuffer.position(oldPos + 9);
          return currentByteBufferAddress + oldPos + 1;
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }

    @Override
    public int read(final byte[] bytes, final int baseOffset, final int maxLength) throws IOException {
      if (bytes == null) {
        throw new NullPointerException();
      }
      if (baseOffset < 0 || maxLength < 0 || maxLength > bytes.length - baseOffset) {
        throw new IndexOutOfBoundsException();
      }
      try {
        // the number of bytes that has been read so far
        int readBytes = 0;
        // the number of bytes to read
        int capacity = maxLength;
        while (capacity > 0) {
          final ByteBuf head = byteBufQueue.peek();
          if (head == null) {
            // end of stream event
            return readBytes == 0 ? -1 : readBytes;
          }
          final int toRead = Math.min(head.readableBytes(), capacity);
          head.readBytes(bytes, baseOffset + readBytes, toRead);
          if (head.readableBytes() == 0) {
            byteBufQueue.take();
            head.release();
          }
          readBytes += toRead;
          capacity -= toRead;
        }
        return readBytes;
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }

    @Override
    public long skip(final long n) throws IOException {
      if (n <= 0) {
        return 0;
      }
      try {
        // the number of bytes that has been skipped so far
        long skippedBytes = 0;
        // the number of bytes to skip
        long toSkip = n;
        while (toSkip > 0) {
          final ByteBuf head = byteBufQueue.peek();
          if (head == null) {
            // end of stream event
            return skippedBytes;
          }
          if (head.readableBytes() > toSkip) {
            head.skipBytes((int) toSkip);
            skippedBytes += toSkip;
            return skippedBytes;
          } else {
            // discard the whole ByteBuf
            skippedBytes += head.readableBytes();
            toSkip -= head.readableBytes();
            byteBufQueue.take();
            head.release();
          }
        }
        return skippedBytes;
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }

    @Override
    public int available() throws IOException {
      try {
        final ByteBuf head = byteBufQueue.peek();
        if (head == null) {
          return 0;
        } else {
          return head.readableBytes();
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }
  }
}
