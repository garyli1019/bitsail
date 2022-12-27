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

package com.bytedance.bitsail.connector.mysql.source;

import com.bytedance.bitsail.base.connector.reader.v1.Boundedness;
import com.bytedance.bitsail.base.connector.reader.v1.Source;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.base.extension.ParallelismComputable;
import com.bytedance.bitsail.base.parallelism.ParallelismAdvice;
import com.bytedance.bitsail.base.serializer.BinarySerializer;
import com.bytedance.bitsail.base.serializer.SimpleBinarySerializer;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.type.BitSailTypeInfoConverter;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.connector.mysql.constant.MysqlConstants;
import com.bytedance.bitsail.connector.mysql.source.reader.MysqlSourceReader;
import com.bytedance.bitsail.connector.mysql.source.split.MysqlSplit;
import com.bytedance.bitsail.connector.mysql.source.split.coordinator.MysqlSourceSplitCoordinator;
import com.bytedance.bitsail.connector.mysql.source.state.MysqlSplitState;

import java.io.IOException;

public class MysqlCDCSource implements Source<Row, MysqlSplit, MysqlSplitState>, ParallelismComputable {

  private BitSailConfiguration jobConf;

  @Override
  public void configure(ExecutionEnviron execution, BitSailConfiguration readerConfiguration) throws IOException {
    this.jobConf = readerConfiguration;
  }

  @Override
  public Boundedness getSourceBoundedness() {
    //TODO: support batch
    return Boundedness.UNBOUNDEDNESS;
  }

  @Override
  public SourceReader<Row, MysqlSplit> createReader(SourceReader.Context readerContext) {
    return new MysqlSourceReader(jobConf, readerContext);
  }

  @Override
  public SourceSplitCoordinator<MysqlSplit, MysqlSplitState> createSplitCoordinator(SourceSplitCoordinator.Context<MysqlSplit, MysqlSplitState> coordinatorContext) {
    return new MysqlSourceSplitCoordinator(coordinatorContext, jobConf);
  }

  @Override
  public BinarySerializer<MysqlSplit> getSplitSerializer() {
    return new SimpleBinarySerializer<>();
  }

  @Override
  public BinarySerializer<MysqlSplitState> getSplitCoordinatorCheckpointSerializer() {
    return new SimpleBinarySerializer<>();
  }

  @Override
  public TypeInfoConverter createTypeInfoConverter() {
    return new BitSailTypeInfoConverter();
  }

  @Override
  public String getReaderName() {
    return MysqlConstants.MYSQL_CDC_CONNECTOR_NAME;
  }

  @Override
  public ParallelismAdvice getParallelismAdvice(BitSailConfiguration commonConf, BitSailConfiguration selfConf, ParallelismAdvice upstreamAdvice) throws Exception {
    return ParallelismAdvice.builder()
        //Currently only support single parallelism binlog reader
        .adviceParallelism(1)
        .enforceDownStreamChain(false)
        .build();
  }
}
