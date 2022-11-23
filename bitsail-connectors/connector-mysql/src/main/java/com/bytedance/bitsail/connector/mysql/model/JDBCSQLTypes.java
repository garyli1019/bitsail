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

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.exception.CommonErrorCode;

public class JDBCSQLTypes {

  /**
   * convert mysql type to an intermedia type
   * @param type
   * @return
   * @throws BitSailException
   */
  public static SqlTypes getSqlTypeFromMysql(final String type) throws BitSailException {
    switch (type) {
      case "bit":
        return SqlTypes.Boolean;
      case "tinyint":
      case "tinyint unsigned":
        return SqlTypes.Short;
      case "smallint":
      case "smallint unsigned":
      case "mediumint":
      case "mediumint unsigned":
      case "int":
      case "int unsigned":
        return SqlTypes.Long;
      case "bigint":
      case "bigint unsigned":
        return SqlTypes.BigInt;
      case "datetime":
      case "timestamp":
        return SqlTypes.Timestamp;
      case "time":
        return SqlTypes.Time;
      case "date":
        return SqlTypes.Date;
      case "year":
        return SqlTypes.Year;
      case "float":
        return SqlTypes.Float;
      case "double":
      case "real":
        return SqlTypes.Double;
      case "decimal":
        return SqlTypes.BigDecimal;
      case "enum":
      case "char":
      case "nvar":
      case "varchar":
      case "nvarchar":
      case "longnvarchar":
      case "longtext":
      case "text":
      case "mediumtext":
      case "string":
      case "longvarchar":
      case "tinytext":
      case "json":
        return SqlTypes.String;
      case "tinyblob":
      case "blob":
      case "mediumblob":
      case "longblob":
      case "binary":
      case "longvarbinary":
      case "varbinary":
        return SqlTypes.Bytes;
      default:
        throw BitSailException.asBitSailException(CommonErrorCode.UNSUPPORTED_COLUMN_TYPE,
            String.format("MySQL: The column data type in your configuration is not support. Column type:[%s]." +
                " Please try to change the column data type or don't transmit this column.", type)
        );
    }
  }

  public enum SqlTypes {
    Boolean,
    Short,
    Int,
    Long,
    Timestamp,
    Time,
    Date,
    Year,
    Float,
    Double,
    Money,
    OtherTypeString,
    String,
    Bytes,
    BigInt,
    BigDecimal,
    Array
  }
}
