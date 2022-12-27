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

package com.bytedance.bitsail.connector.mysql.source.config;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;
import java.util.Properties;

@Getter
@Builder
public class MysqlConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String hostname;
  private final int port;
  private final String username;
  private final String password;

  // debezium configuration
  private final Properties dbzProperties;
  private final Configuration dbzConfiguration;
  private final MySqlConnectorConfig dbzMySqlConnectorConfig;

  public static MysqlConfig fromJobConf(BitSailConfiguration jobConf) {
    Properties props = extractProps(jobConf);
    Configuration config = Configuration.from(props);
    return MysqlConfig.builder()
        .hostname("123")
        .port(1)
        .username("123")
        .password("123")
        .dbzProperties(props)
        .dbzConfiguration(config)
        .dbzMySqlConnectorConfig(new MySqlConnectorConfig(config))
        .build();
  }

  public static Properties extractProps(BitSailConfiguration jobConf) {
    Properties props = new Properties();
    props.setProperty("p1", "v1");
    return props;
  }

}
