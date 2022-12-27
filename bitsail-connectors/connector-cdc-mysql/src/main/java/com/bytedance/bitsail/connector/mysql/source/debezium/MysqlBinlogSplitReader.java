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

package com.bytedance.bitsail.connector.mysql.source.debezium;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.mysql.source.config.MysqlConfig;
import com.bytedance.bitsail.connector.mysql.source.split.MysqlSplit;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.mysql.MySqlBinaryProtocolFieldReader;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlErrorHandler;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSource;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetrics;
import io.debezium.connector.mysql.MySqlTaskContext;
import io.debezium.connector.mysql.MySqlTextProtocolFieldReader;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static io.debezium.relational.RelationalDatabaseConnectorConfig.DATABASE_NAME;

/**
 * Reader that actually execute the Debezium task.
 */
public class MysqlBinlogSplitReader {

  private static final Logger LOG = LoggerFactory.getLogger(MysqlBinlogSplitReader.class);

  boolean isRunning;

  private final MysqlConfig mysqlConfig;

  private ChangeEventQueue<DataChangeEvent> queue;

  private EventDispatcher<MySqlPartition, TableId> dispatcher;

  private MySqlConnectorConfig connectorConfig;

  private volatile MySqlTaskContext taskContext;

  private volatile MySqlConnection connection;

  private volatile ErrorHandler errorHandler;

  private volatile MySqlDatabaseSchema schema;

  private final ExecutorService executorService;

  private MySqlStreamingChangeEventSource dbzSource;

  private BinaryLogClient binaryLogClient;

  private List<SourceRecord> batch;
  private Iterator<SourceRecord> recordIterator;

  private MysqlSplit split;

  private final int subtaskId;

  public MysqlBinlogSplitReader(BitSailConfiguration jobConf, int subtaskId) {
    this.mysqlConfig = MysqlConfig.fromJobConf(jobConf);
    // handle configuration
    this.connectorConfig = mysqlConfig.getDbzMySqlConnectorConfig();
    this.subtaskId = subtaskId;
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("mysql-binlog-reader-" + this.subtaskId).build();
    this.executorService = Executors.newSingleThreadExecutor(threadFactory);
  }

  public void readSplit(MysqlSplit split) {
    this.split = split;
    MySqlOffsetContext offsetContext = DebeziumHelper.loadOffsetContext(connectorConfig, split);
    final TopicNamingStrategy topicNamingStrategy = connectorConfig.getTopicNamingStrategy(MySqlConnectorConfig.TOPIC_NAMING_STRATEGY);
    final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjustmentMode().createAdjuster();
    final MySqlValueConverters valueConverters = DebeziumHelper.getValueConverters(connectorConfig);

    this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
        .pollInterval(connectorConfig.getPollInterval())
        .maxBatchSize(connectorConfig.getMaxBatchSize())
        .maxQueueSize(connectorConfig.getMaxQueueSize())
        .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
        .loggingContextSupplier(() -> taskContext.configureLoggingContext("mysql-connector-task"))
        .buffering()
        .build();
    this.batch = new ArrayList<>();
    this.recordIterator = this.batch.iterator();
    this.errorHandler = new MySqlErrorHandler(connectorConfig, queue);

    final MySqlEventMetadataProvider metadataProvider = new MySqlEventMetadataProvider();

    final Configuration dbzConfiguration = mysqlConfig.getDbzConfiguration();
    this.connection = new MySqlConnection(
        new MySqlConnection.MySqlConnectionConfiguration(dbzConfiguration), connectorConfig.useCursorFetch() ?
        new MySqlBinaryProtocolFieldReader(connectorConfig)
        : new MySqlTextProtocolFieldReader(connectorConfig));

    DebeziumHelper.validateBinlogConfiguration(connectorConfig, connection);

    final boolean tableIdCaseInsensitive = connection.isTableIdCaseSensitive();

    this.schema = new MySqlDatabaseSchema(connectorConfig, valueConverters, topicNamingStrategy, schemaNameAdjuster, tableIdCaseInsensitive);
    this.taskContext = new MySqlTaskContext(connectorConfig, schema);

    this.binaryLogClient = new BinaryLogClient(
        connectorConfig.hostname(),
        connectorConfig.port(),
        connectorConfig.username(),
        connectorConfig.password());

    this.dispatcher = new EventDispatcher<>(
        connectorConfig,
        topicNamingStrategy,
        schema,
        queue,
        connectorConfig.getTableFilters().dataCollectionFilter(),
        DataChangeEvent::new,
        null,
        metadataProvider,
        connectorConfig.createHeartbeat(
            topicNamingStrategy,
            schemaNameAdjuster,
            () -> connection,
            exception -> {
              String sqlErrorId = exception.getSQLState();
              switch (sqlErrorId) {
                case "42000":
                  // error_er_dbaccess_denied_error, see https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_dbaccess_denied_error
                  throw new DebeziumException("Could not execute heartbeat action query (Error: " + sqlErrorId + ")", exception);
                case "3D000":
                  // error_er_no_db_error, see https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_no_db_error
                  throw new DebeziumException("Could not execute heartbeat action query (Error: " + sqlErrorId + ")", exception);
                default:
                  break;
              }
            }),
        schemaNameAdjuster);

    this.dbzSource = new MySqlStreamingChangeEventSource(
        connectorConfig,
        connection,
        dispatcher,
        errorHandler,
        Clock.SYSTEM,
        taskContext, // reuse binary log client
        new MySqlStreamingChangeEventSourceMetrics(
            taskContext, queue, metadataProvider)
    );
    executorService.submit(
        () -> {
          try {
            dbzSource.execute(
                new BinlogChangeEventSourceContext(),
                new MySqlPartition(
                    connectorConfig.getLogicalName(), dbzConfiguration.getString(DATABASE_NAME.name())),
                offsetContext);
          } catch (Exception e) {
            isRunning = false;
            LOG.error("Execute debezium binlog reader failed", e);
          }
        });
  }

  public void close() {
    try {
      if (this.connection != null) {
        this.connection.close();
      }
      if (this.binaryLogClient != null) {
        this.binaryLogClient.disconnect();
      }
      if (executorService != null) {
        executorService.shutdown();
      }
      isRunning = false;
    } catch (Exception e) {
      LOG.error("Failed to close MysqlBinlogSplitReader, exist anyway.", e);
    }
  }

  private class BinlogChangeEventSourceContext
      implements ChangeEventSource.ChangeEventSourceContext {
    @Override
    public boolean isRunning() {
      return isRunning;
    }
  }

  public boolean isCompleted() {
    return !isRunning;
  }

  public SourceRecord poll() {
    return this.recordIterator.next();
  }

  public boolean hasNext() throws InterruptedException {
    if (this.recordIterator.hasNext()) {
      return true;
    } else {
      return pollNextBatch();
    }
  }

  private boolean pollNextBatch() throws InterruptedException {
    if (isRunning) {
      List<DataChangeEvent> dbzRecords = queue.poll();
      // TODO: handle DBlog algorithm to de-dup here
      this.batch = new ArrayList<>();
      for (DataChangeEvent event : dbzRecords) {
        this.batch.add(event.getRecord());
      }
      this.recordIterator = this.batch.iterator();
      return true;
    }
    return false;
  }
}
