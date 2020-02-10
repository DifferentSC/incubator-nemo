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
package org.apache.nemo.examples.beam;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class LittleEndianLongCoder extends AtomicCoder<Long> {

  private final byte[] buffer = new byte[8];

  @Override
  public void encode(final Long v, final OutputStream outStream) throws CoderException, IOException {
    buffer[0] = (byte)(v >>> 0);
    buffer[1] = (byte)(v >>> 8);
    buffer[2] = (byte)(v >>> 16);
    buffer[3] = (byte)(v >>> 24);
    buffer[4] = (byte)(v >>> 32);
    buffer[5] = (byte)(v >>> 40);
    buffer[6] = (byte)(v >>> 48);
    buffer[7] = (byte)(v >>> 56);
    outStream.write(buffer, 0, 8);
  }

  @Override
  public Long decode(final InputStream inStream) throws CoderException, IOException {
    inStream.read(buffer, 0, 8);
    return (((long)buffer[7] << 56) +
      ((long)(buffer[6] & 255) << 48) +
      ((long)(buffer[5] & 255) << 40) +
      ((long)(buffer[4] & 255) << 32) +
      ((long)(buffer[3] & 255) << 24) +
      ((buffer[2] & 255) << 16) +
      ((buffer[1] & 255) <<  8) +
      ((buffer[0] & 255) <<  0));
  }
}
