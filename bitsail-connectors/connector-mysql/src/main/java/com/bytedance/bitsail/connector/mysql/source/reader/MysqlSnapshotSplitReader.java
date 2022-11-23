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

package com.bytedance.bitsail.connector.mysql.source.reader;

import com.bytedance.bitsail.base.connector.reader.v1.SourceEvent;
import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.mysql.source.split.MysqlSplit;

import java.util.List;

public class MysqlSnapshotSplitReader implements SourceReader<Row, MysqlSplit> {

  private final int subTaskId;

  private final BitSailConfiguration jobConf;

  public MysqlSnapshotSplitReader(BitSailConfiguration jobConf, int subTaskId) {
    this.subTaskId = subTaskId;
    this.jobConf = jobConf;
  }

  @Override
  public void start() {

  }

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws Exception {

  }

  @Override
  public void addSplits(List<MysqlSplit> splits) {

  }

  @Override
  public boolean hasMoreElements() {
    return false;
  }

  @Override
  public void notifyNoMoreSplits() {
    SourceReader.super.notifyNoMoreSplits();
  }

  @Override
  public void handleSourceEvent(SourceEvent sourceEvent) {
    SourceReader.super.handleSourceEvent(sourceEvent);
  }

  @Override
  public List<MysqlSplit> snapshotState(long checkpointId) {
    return null;
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    SourceReader.super.notifyCheckpointComplete(checkpointId);
  }

  @Override
  public void close() throws Exception {

  }
}
