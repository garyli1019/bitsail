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

package com.bytedance.bitsail.connector.mysql.utils;

import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.connector.mysql.constant.MysqlConstant;

import com.google.common.base.Strings;

import java.util.List;

public class SqlGeneratorUtils {

  public static String generateSelectSqlTemplate(String splitPK, List<ColumnInfo> columns, String filter) {
    String tableToken = "%s";
    StringBuilder sql = new StringBuilder("SELECT ");
    for (ColumnInfo column : columns) {
      String columnName = column.getName();
      sql.append(appendQuote(columnName, MysqlConstant.DB_QUOTE)).append(",");
    }
    sql.deleteCharAt(sql.length() - 1);
    sql.append(" FROM ").append(tableToken).append(" WHERE ");
    if (null != filter) {
      sql.append("(").append(filter).append(")");
      sql.append(" AND ");
    }
    sql.append("(").append(appendQuote(splitPK, MysqlConstant.DB_QUOTE)).append(" >= ? AND ").append(appendQuote(splitPK, MysqlConstant.DB_QUOTE)).append(" < ?)");
    return sql.toString();
  }

  public static String appendQuote(final String column, final String quota) {
    return quota.concat(column).concat(quota);
  }

  public static String getQuoteTable(final String table) {
    if (Strings.isNullOrEmpty(table)) {
      return table;
    }
    String[] parts = table.split("\\.");
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < parts.length; ++i) {
      if (i != 0) {
        stringBuilder.append(".");
      }
      stringBuilder.append(appendQuote(parts[i], MysqlConstant.DB_QUOTE));
    }
    return stringBuilder.toString();
  }
}
