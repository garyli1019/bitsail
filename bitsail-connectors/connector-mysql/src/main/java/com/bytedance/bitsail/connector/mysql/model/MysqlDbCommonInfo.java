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

package com.bytedance.bitsail.connector.mysql.model;

public class MysqlDbCommonInfo implements DbCommonInfo {

  public static final String DRIVER_NAME = "org.mariadb.jdbc.Driver";
  public static final String DB_QUOTE = "`";
  public static final String VALUE_QUOTE = "\"";

  @Override
  public String getDriverName() {
    return DRIVER_NAME;
  }

  @Override
  public String getFieldQuote() {
    return DB_QUOTE;
  }

  @Override
  public String getValueQuote() {
    return VALUE_QUOTE;
  }
}
