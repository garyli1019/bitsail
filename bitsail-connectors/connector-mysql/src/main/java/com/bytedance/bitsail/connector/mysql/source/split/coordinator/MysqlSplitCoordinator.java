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

package com.bytedance.bitsail.connector.mysql.source.split.coordinator;

import com.bytedance.bitsail.base.connector.reader.v1.SourceEvent;
import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.connector.mysql.error.MysqlErrorCode;
import com.bytedance.bitsail.connector.mysql.model.ClusterInfo;
import com.bytedance.bitsail.connector.mysql.model.DbClusterInfo;
import com.bytedance.bitsail.connector.mysql.model.JDBCSQLTypes;
import com.bytedance.bitsail.connector.mysql.option.MysqlReaderOptions;
import com.bytedance.bitsail.connector.mysql.source.split.MysqlSplit;
import com.bytedance.bitsail.connector.mysql.source.split.strategy.SplitStrategy;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class MysqlSplitCoordinator implements SourceSplitCoordinator<MysqlSplit, EmptyState> {

  private static final Logger LOG = LoggerFactory.getLogger(MysqlSplitCoordinator.class);
  private final SourceSplitCoordinator.Context<MysqlSplit, EmptyState> context;
  private final BitSailConfiguration jobConf;

  private final Map<Integer, Set<MysqlSplit>> splitAssignmentPlan;

  // TODO: relocate
  private String initSql;

  private String splitMode;

  private DbClusterInfo dbClusterInfo;
  private String queryTemplateFormat;
  private RowTypeInfo rowTypeInfo;

  private int fetchSize;
  private String schema;

  private final int parallelism;

  public MysqlSplitCoordinator(Context<MysqlSplit, EmptyState> context, BitSailConfiguration jobConf) {
    this.context = context;
    this.jobConf = jobConf;
    this.splitAssignmentPlan = Maps.newConcurrentMap();
    this.parallelism = context.totalParallelism();
  }

  @Override
  public void start() {
    init();
    getTableStats();
    assignSplit();

  }

  @Override
  public void addReader(int subtaskId) {

  }

  @Override
  public void addSplitsBack(List<MysqlSplit> splits, int subtaskId) {

  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

  }

  @Override
  public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    SourceSplitCoordinator.super.handleSourceEvent(subtaskId, sourceEvent);
  }

  @Override
  public EmptyState snapshotState() throws Exception {
    return null;
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    SourceSplitCoordinator.super.notifyCheckpointComplete(checkpointId);
  }

  @Override
  public void close() {

  }

  private void getTableStats() {

  }

  private void assignSplit() {

  }

  private void init() {
    String customizedSQL = jobConf.getUnNecessaryOption(MysqlReaderOptions.CUSTOMIZED_SQL, null);
    boolean useCustomizedSQL = customizedSQL != null && customizedSQL.length() > 0;

    String userName = jobConf.getNecessaryOption(MysqlReaderOptions.USER_NAME, MysqlErrorCode.REQUIRED_VALUE);
    String password = jobConf.getNecessaryOption(MysqlReaderOptions.PASSWORD, MysqlErrorCode.REQUIRED_VALUE);

    String initSql = jobConf.get(MysqlReaderOptions.INIT_SQL);
    if (!Strings.isNullOrEmpty(initSql)) {
      LOG.info("Init sql is " + initSql);
    }

    List<ColumnInfo> columns = jobConf.getNecessaryOption(MysqlReaderOptions.COLUMNS, MysqlErrorCode.REQUIRED_VALUE);
    String splitPK = null;
    String table = null;
    String schema = null;
    String tableWithSchema = null;
    JDBCSQLTypes.SqlTypes splitType = null;

    if (!useCustomizedSQL) {
      splitPK = jobConf.getNecessaryOption(MysqlReaderOptions.SPLIT_PK, MysqlErrorCode.REQUIRED_VALUE);
      String splitPkType = getOrDefaultSplitType(jobConf, columns, splitPK);
      jobConf.set(MysqlReaderOptions.SPLIT_PK_JDBC_TYPE, splitPkType);
      splitType = JDBCSQLTypes.getSqlTypeFromMysql(splitPkType);
      table = jobConf.get(MysqlReaderOptions.TABLE_NAME);
      schema = jobConf.getUnNecessaryOption(MysqlReaderOptions.TABLE_SCHEMA, MysqlReaderOptions.TABLE_SCHEMA.defaultValue());
    }

    List<ClusterInfo> connections = jobConf.getNecessaryOption(MysqlReaderOptions.CONNECTIONS, MysqlErrorCode.REQUIRED_VALUE);
    String connectionParameters = jobConf.getUnNecessaryOption(MysqlReaderOptions.CONNECTION_PARAMETERS, null);
    fillClusterInfo(connections, table, connectionParameters);

    // Unnecessary values
    int fetchSize = jobConf.getUnNecessaryOption(MysqlReaderOptions.READER_FETCH_SIZE, 10000);
    int readerParallelismNum = useCustomizedSQL ? -1 : jobConf.getUnNecessaryOption(MysqlReaderOptions.READER_PARALLELISM_NUM, -1);
    String filter = useCustomizedSQL ? null : jobConf.getUnNecessaryOption(MysqlReaderOptions.FILTER, null);
    List<String> shardKey = useCustomizedSQL ? null : jobConf.get(MysqlReaderOptions.SHARD_KEY);
    this.splitMode = jobConf.getUnNecessaryOption(MysqlReaderOptions.SHARD_SPLIT_MODE, SplitStrategy.Mode.accurate.name());
    // Generate from conf params
    DbClusterInfo dbClusterInfo = new DbClusterInfo(userName, password, schema, splitPK, shardKey, 1, 1, 1, 120, connections);

    // Init member
    this.dbClusterInfo = dbClusterInfo;
    this.fetchSize = fetchSize;
    //rowTypeInfo = NativeFlinkTypeInfoUtil.getRowTypeInformation(columns, createTypeInfoConverter());
    LOG.info("Row Type Info: " + rowTypeInfo);
    LOG.info("Validate plugin configuration parameters finished.");

  }

  String getOrDefaultSplitType(BitSailConfiguration jobConf, List<ColumnInfo> columns, String splitPK) {
    String splitPkType = null;
    if (jobConf.fieldExists(MysqlReaderOptions.SPLIT_PK_JDBC_TYPE)) {
      splitPkType = jobConf.get(MysqlReaderOptions.SPLIT_PK_JDBC_TYPE).toLowerCase();
    } else {
      for (ColumnInfo columnInfo : columns) {
        String name = columnInfo.getName();
        String type = columnInfo.getType().toLowerCase();
        if (name.equalsIgnoreCase(splitPK)) {
          splitPkType = type;
          LOG.info("split key is '{}', type is '{}'", splitPK, type);
          break;
        }
      }
      if (splitPkType == null) {
        LOG.warn("split key '{}' is not found in field mapping, try use int split", splitPK);
        return "int";
      }
    }
    return splitPkType;
  }

  private static void fillClusterInfo(List<ClusterInfo> clusterInfos, String table, String connectionParameters) {
    if (StringUtils.isNotEmpty(table)) {
      for (ClusterInfo connection : clusterInfos) {
        connection.setTableNames(table);
      }
    }
    if (StringUtils.isNotEmpty(connectionParameters)) {
      for (ClusterInfo connection : clusterInfos) {
        connection.setConnectionParameters(connectionParameters);
      }
    }
  }

}
