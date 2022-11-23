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

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.connector.mysql.option.MysqlReaderOptions;

import java.util.List;

public class MysqlUtils {
  /**
   * @param splitPK: split primary key
   * @param columns: columns
   * @param filter:  filter
   */
  String genSqlTemplate(String splitPK, List<ColumnInfo> columns, String filter, String quote) {
    String tableToken = "%s";
    StringBuilder sql = new StringBuilder("SELECT ");
    for (ColumnInfo column : columns) {
      String columnName = column.getName();
      sql.append(getQuoteColumn(columnName, quote)).append(",");
    }
    sql.deleteCharAt(sql.length() - 1);
    sql.append(" FROM ").append(tableToken).append(" WHERE ");
    if (null != filter) {
      sql.append("(").append(filter).append(")");
      sql.append(" AND ");
    }
    sql.append("(").append(getQuoteColumn(splitPK, quote)).append(" >= ? AND ").append(getQuoteColumn(splitPK, quote)).append(" < ?)");
    return sql.toString();
  }

  String getQuoteColumn(final String column, String quote) {
    return quote.concat(column).concat(quote);
  }


}
