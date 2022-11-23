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

package com.bytedance.bitsail.connector.mysql.error;

import com.bytedance.bitsail.common.exception.ErrorCode;

public enum MysqlErrorCode implements ErrorCode {

  REQUIRED_VALUE("Mysql-01", "You missed parameter which is required, please check your configuration."),
  ILLEGAL_VALUE("Mysql-02", "The parameter value in your configuration is not valid."),
  INTERNAL_ERROR("Mysql-03", "Internal error occurred, which is usually a BitSail bug."),
  CONNECTION_ERROR("Mysql-04", "Error occurred while getting database connection."),
  DYNAMIC_EXTRA_PARTITION_ERROR("Mysql-05", "Dynamic extra partition error occur, value can't be null in dynamic partitions");


  private final String code;

  private final String describe;

  MysqlErrorCode(String code, String describe) {
    this.code = code;
    this.describe = describe;
  }

  @Override
  public String getCode() {
    return code;
  }

  @Override
  public String getDescription() {
    return describe;
  }

  @Override
  public String toString() {
    return String.format("Code:[%s], Describe:[%s]", this.code,
        this.describe);
  }
}
