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

package com.bytedance.bitsail.connector.mysql.source.split.strategy;

import com.bytedance.bitsail.connector.mysql.model.DbClusterInfo;
import com.bytedance.bitsail.connector.mysql.model.DbCommonInfo;
import com.bytedance.bitsail.connector.mysql.model.DbShardInfo;
import com.bytedance.bitsail.connector.mysql.model.JDBCSQLTypes;

import java.util.List;

public abstract class AbstractSplitStrategy implements SplitStrategy {

  private final DbCommonInfo dbCommonInfo;

  private final DbClusterInfo dbClusterInfo;

  private final List<DbShardInfo> dbShardInfos;

  private final JDBCSQLTypes.SqlTypes sqlTypes;



  public AbstractSplitStrategy(DbCommonInfo dbCommonInfo, DbClusterInfo dbClusterInfo,
                               List<DbShardInfo> dbShardInfos, JDBCSQLTypes.SqlTypes sqlTypes) {
    this.dbCommonInfo = dbCommonInfo;
    this.dbClusterInfo = dbClusterInfo;
    this.dbShardInfos = dbShardInfos;
    this.sqlTypes = sqlTypes;
  }

  @Override
  public void open() {

  }

  @Override
  public void assign() {

  }
}
