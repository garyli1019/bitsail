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
import com.bytedance.bitsail.connector.mysql.model.ClusterInfo;
import com.bytedance.bitsail.connector.mysql.model.ConnectionInfo;
import com.bytedance.bitsail.connector.mysql.option.MysqlReaderOptions;
import com.bytedance.bitsail.connector.mysql.source.config.MysqlConfig;
import com.bytedance.bitsail.connector.mysql.source.offset.BinlogOffset;
import com.bytedance.bitsail.connector.mysql.source.split.MysqlSplit;
import com.bytedance.bitsail.connector.mysql.utils.TestDatabase;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

public class TestConnection {
  String username = "root";
  String password = "mysqlpw";
  String host = "localhost";
  int port = 55003;

  @Test
  public void testContainer() throws SQLException {


    Connection connection = DriverManager.getConnection(
        getJdbcUrl(), username, password);
    Statement statement = connection.createStatement();
    boolean result = statement.execute("SHOW DATABASES;");

  }

  @Test
  public void testReader() throws InterruptedException {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();

    ConnectionInfo connectionInfo = ConnectionInfo.builder()
        .host(host)
        .port(port)
        .url(getJdbcUrl())
        .build();
    ClusterInfo clusterInfo = ClusterInfo.builder()
        .master(connectionInfo)
        .build();

    jobConf.set(MysqlReaderOptions.CONNECTIONS, Lists.newArrayList(clusterInfo));
    jobConf.set(MysqlReaderOptions.USER_NAME, username);
    jobConf.set(MysqlReaderOptions.PASSWORD, password);
    jobConf.set("job.reader.debezium.database.useSSL", "false");
    jobConf.set("job.reader.debezium.database.allowPublicKeyRetrieval", "true");
    jobConf.set("job.reader.debezium.database.server.id", "123");
    jobConf.set("job.reader.debezium.schema.history.internal", "io.debezium.relational.history.MemorySchemaHistory");
    setTopicNamingStrategy(jobConf);

    MysqlBinlogSplitReader reader = new MysqlBinlogSplitReader(jobConf, 0);
    MysqlSplit split = new MysqlSplit("split-1", BinlogOffset.earliest(), BinlogOffset.boundless());
    reader.readSplit(split);
    int maxPeriod = 0;
    while (maxPeriod <= 1) {
      if (reader.hasNext()) {
        reader.poll();
      }
      maxPeriod ++;
      TimeUnit.SECONDS.sleep(10);
    }

  }

  public String getJdbcUrl() {
    return "jdbc:mysql://" + host + ":" + port;
  }

  private void setTopicNamingStrategy(BitSailConfiguration jobConf) {
    jobConf.set(MysqlConfig.DEBEZIUM_PREFIX + "topic.prefix", "test1");
    jobConf.set(MysqlConfig.DEBEZIUM_PREFIX + "topic.delimiter", ".");
    jobConf.set(MysqlConfig.DEBEZIUM_PREFIX + "topic.cache.size", "10000");
    jobConf.set(MysqlConfig.DEBEZIUM_PREFIX + "topic.transaction", "transaction");
    jobConf.set(MysqlConfig.DEBEZIUM_PREFIX + "topic.heartbeat.prefix", "__debezium-heartbeat");
  }
}
