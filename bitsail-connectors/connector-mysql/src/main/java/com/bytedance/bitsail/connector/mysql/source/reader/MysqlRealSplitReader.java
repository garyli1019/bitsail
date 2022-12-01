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

import com.bytedance.bitsail.connector.mysql.converter.JdbcValueConverter;
import com.bytedance.bitsail.connector.mysql.source.split.MysqlSnapshotSplit;

import org.apache.flink.types.Row;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class MysqlRealSplitReader {

  private ResultSet resultSet;

  private ResultSetMetaData metaData;

  private transient JdbcValueConverter jdbcValueConverter;



  public MysqlRealSplitReader() {
    this.jdbcValueConverter = new JdbcValueConverter();
  }

  public void readSplit(MysqlSnapshotSplit split) {

  }

  public Row fetchNext() {

  }

  void setStatementRange(PreparedStatement statement, String start, String end) throws SQLException {
    statement.setString(1, start);
    statement.setString(2, end);
  }

}
