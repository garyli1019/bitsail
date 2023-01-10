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
import com.bytedance.bitsail.connector.mysql.container.MySQLContainerMariadbAdapter;
import com.bytedance.bitsail.connector.mysql.model.ClusterInfo;
import com.bytedance.bitsail.connector.mysql.model.ConnectionInfo;
import com.bytedance.bitsail.connector.mysql.option.MysqlReaderOptions;
import com.bytedance.bitsail.connector.mysql.source.config.MysqlConfig;
import com.bytedance.bitsail.connector.mysql.source.offset.BinlogOffset;
import com.bytedance.bitsail.connector.mysql.source.split.MysqlSplit;
import com.bytedance.bitsail.connector.mysql.utils.TestDatabase;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class MysqlBinlogSplitReaderTest {

  private static final Logger LOG = LoggerFactory.getLogger(MysqlBinlogSplitReaderTest.class);

  private static final String MYSQL_DOCKER_IMAGER = "mysql:8.0.29";

  private static final String TEST_USERNAME = "user1";
  private static final String TEST_PASSWORD = "password1";

  private MySQLContainer<?> container;

  @Before
  public void before() {
    container = new MySQLContainerMariadbAdapter<>(DockerImageName.parse(MYSQL_DOCKER_IMAGER))
        .withUrlParam("permitMysqlScheme", null)
        .withInitScript("scripts/jdbc_to_print.sql")
        .withUsername(TEST_USERNAME)
        .withPassword(TEST_PASSWORD)
        .withLogConsumer(new Slf4jLogConsumer(LOG));

    Startables.deepStart(Stream.of(container)).join();
  }

  @After
  public void after() {
    container.close();
  }

  @Test
  public void testDatabaseConnection() {
    TestDatabase db = new TestDatabase(container, "test", TEST_USERNAME, TEST_PASSWORD);
    db.executeSql("SHOW TABLES;");


  }

  @Test
  public void testBinlogReader() throws InterruptedException {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    TestDatabase database = new TestDatabase(container, "test", TEST_USERNAME, TEST_PASSWORD);

    ConnectionInfo connectionInfo = ConnectionInfo.builder()
        .host(database.getMySQLContainer().getHost())
        .port(database.getMySQLContainer().getFirstMappedPort())
        .url(database.getMySQLContainer().getJdbcUrl())
        .build();
    ClusterInfo clusterInfo = ClusterInfo.builder()
        .master(connectionInfo)
        .build();

    jobConf.set(MysqlReaderOptions.CONNECTIONS, Lists.newArrayList(clusterInfo));
    jobConf.set(MysqlReaderOptions.USER_NAME, database.getUsername());
    jobConf.set(MysqlReaderOptions.PASSWORD, database.getPassword());
    setTopicNamingStrategy(jobConf);

    MysqlBinlogSplitReader reader = new MysqlBinlogSplitReader(jobConf, 0);
    MysqlSplit split = new MysqlSplit("split-1", BinlogOffset.earliest(), BinlogOffset.boundless());
    reader.readSplit(split);

    TimeUnit.SECONDS.sleep(10);
    reader.close();
  }

  private void setTopicNamingStrategy(BitSailConfiguration jobConf) {
    jobConf.set(MysqlConfig.DEBEZIUM_PREFIX + "topic.prefix", "test1");
    jobConf.set(MysqlConfig.DEBEZIUM_PREFIX + "topic.delimiter", ".");
    jobConf.set(MysqlConfig.DEBEZIUM_PREFIX + "topic.cache.size", "10000");
    jobConf.set(MysqlConfig.DEBEZIUM_PREFIX + "topic.transaction", "transaction");
    jobConf.set(MysqlConfig.DEBEZIUM_PREFIX + "topic.heartbeat.prefix", "__debezium-heartbeat");
  }

}
