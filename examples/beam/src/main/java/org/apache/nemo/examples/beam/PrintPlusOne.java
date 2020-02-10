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
import org.slf4j.Logger;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.LoggerFactory;

public final class PrintPlusOne {
  /**
   * Private Constructor.
   */
  private PrintPlusOne() {
  }

  /**
   * Main Function
   *
   * @param args  arguments
   */
  public static void main(final String[] args) {
    final Logger LOG = LoggerFactory.getLogger(PrintPlusOne.class.getName());
    final String inputFilePath = args[0];

    final PipelineOptions options = NemoPipelineOptionsFactory.create();
    options.setJobName("Offheap_demo");

    final Pipeline p = Pipeline.create(options);

    // processing in native env.
    try {
      GenericSourceSink.read(p, inputFilePath)
        .apply(MapElements.via(new SimpleFunction<String, Long>() {
          @Override
          public Long apply(final String line) {
            Long value = Long.parseLong(line);
            return value;
          }
        }))
        .apply(MapElements.via(new SimpleFunction<Long, Void>() { // the return type Long is not used but specified;
          @Override
          public Void apply(final Long address) {
            System.loadLibrary("NativeFunctions");
            final NativeFunctions nf = new NativeFunctions();
            nf.printPlusOne(address);
            return null;
          }
        }));
    } catch (Exception e){
      throw new RuntimeException(e);
    }

    p.run();

  }
}
