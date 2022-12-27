/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.connector.mysql.source.reader;

import com.bytedance.bitsail.base.connector.reader.v1.SourceEvent;
import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.mysql.source.debezium.MysqlBinlogSplitReader;
import com.bytedance.bitsail.connector.mysql.source.event.BinlogCompleteAckEvent;
import com.bytedance.bitsail.connector.mysql.source.split.MysqlSplit;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public class MysqlSourceReader implements SourceReader<Row, MysqlSplit> {

  private static final Logger LOG = LoggerFactory.getLogger(MysqlSourceReader.class);

  private final Queue<MysqlSplit> remainSplits;

  private MysqlBinlogSplitReader reader;

  public MysqlSourceReader(BitSailConfiguration jobConf, SourceReader.Context readerContext) {
    this.remainSplits = new ArrayDeque<>();
    this.reader = new MysqlBinlogSplitReader(jobConf, readerContext.getIndexOfSubtask());
  }

  @Override
  public void start() {
    //start debezium streaming reader and send data to queue
    if (!remainSplits.isEmpty()) {
      MysqlSplit curSplit = remainSplits.poll();
      LOG.info("submit split to binlog reader: {}", curSplit.toString());
      this.reader.readSplit(curSplit);
    }
  }

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws Exception {
    // poll from reader
    if (!this.reader.isCompleted() && this.reader.hasNext()) {
      SourceRecord record = this.reader.poll();
      LOG.info(record.toString());
    }
    //TODO: Convert source record to Row and collect
  }

  @Override
  public void addSplits(List<MysqlSplit> splits) {
    splits.forEach(e -> LOG.info("Add split: {}", e.toString()));
    remainSplits.addAll(splits);
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
    if (sourceEvent instanceof BinlogCompleteAckEvent) {
      LOG.info("Binlog completed acked by coordinator.");
    }
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
  public void close() {
    if (this.reader != null && !this.reader.isCompleted()) {
      this.reader.close();
      LOG.info("Reader close successfully");
    }
  }
}
