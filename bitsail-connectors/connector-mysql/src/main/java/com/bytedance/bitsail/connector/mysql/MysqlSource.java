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

package com.bytedance.bitsail.connector.mysql;

import com.bytedance.bitsail.base.connector.reader.v1.Boundedness;
import com.bytedance.bitsail.base.connector.reader.v1.Source;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.base.extension.ParallelismComputable;
import com.bytedance.bitsail.base.parallelism.ParallelismAdvice;
import com.bytedance.bitsail.base.serializer.BinarySerializer;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.type.filemapping.FileMappingTypeInfoConverter;
import com.bytedance.bitsail.common.util.TypeConvertUtil;
import com.bytedance.bitsail.connector.mysql.option.MysqlReaderOptions;
import com.bytedance.bitsail.connector.mysql.source.split.MysqlSplit;
import com.bytedance.bitsail.connector.mysql.source.split.coordinator.MysqlSplitCoordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MysqlSource implements Source<Row, MysqlSplit, EmptyState>, ParallelismComputable {

  private static final Logger LOG = LoggerFactory.getLogger(MysqlSource.class);

  private BitSailConfiguration jobConf;

  @Override
  public void configure(ExecutionEnviron execution, BitSailConfiguration readerConfiguration) throws IOException {
    this.jobConf = readerConfiguration;
  }

  @Override
  public Boundedness getSourceBoundedness() {
    return Boundedness.BOUNDEDNESS;
  }

  @Override
  public SourceReader<Row, MysqlSplit> createReader(SourceReader.Context readerContext) {
    return null;
  }

  @Override
  public SourceSplitCoordinator<MysqlSplit, EmptyState> createSplitCoordinator(SourceSplitCoordinator.Context<MysqlSplit, EmptyState> coordinatorContext) {
    return new MysqlSplitCoordinator(coordinatorContext, jobConf);
  }

  @Override
  public BinarySerializer<MysqlSplit> getSplitSerializer() {
    return Source.super.getSplitSerializer();
  }

  @Override
  public BinarySerializer<EmptyState> getSplitCoordinatorCheckpointSerializer() {
    return Source.super.getSplitCoordinatorCheckpointSerializer();
  }

  @Override
  public TypeInfoConverter createTypeInfoConverter() {
    return new FileMappingTypeInfoConverter(TypeConvertUtil.StorageEngine.mysql.name());
  }

  @Override
  public String getReaderName() {
    return "mysql";
  }

  @Override
  public ParallelismAdvice getParallelismAdvice(BitSailConfiguration commonConf, BitSailConfiguration selfConf, ParallelismAdvice upstreamAdvice) throws Exception {
    int parallelism;
    if (selfConf.fieldExists(MysqlReaderOptions.READER_PARALLELISM_NUM)) {
      parallelism = selfConf.get(MysqlReaderOptions.READER_PARALLELISM_NUM);
    } else {
      //TODO add para calculator
      parallelism = 1;
    }
    return new ParallelismAdvice(false, parallelism);
  }
}
