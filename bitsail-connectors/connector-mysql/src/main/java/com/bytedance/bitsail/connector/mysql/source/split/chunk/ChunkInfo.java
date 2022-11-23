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

package com.bytedance.bitsail.connector.mysql.source.split.chunk;

import javax.annotation.Nullable;

import java.util.Objects;

public class ChunkInfo {

  private final @Nullable Object start;

  private final @Nullable Object end;

  private final @Nullable String customizedSQL;

  public static ChunkInfo all() {
    return new ChunkInfo(null, null, null);
  }

  public static ChunkInfo withRange(Object start, Object end) {
    return new ChunkInfo(start, end, null);
  }

  public static ChunkInfo withCustomizedSQL(String customizedSQL) {
    return new ChunkInfo(null, null, customizedSQL);
  }

  private ChunkInfo(@Nullable Object start, @Nullable Object end, @Nullable String customizedSQL) {
    this.start = start;
    this.end = end;
    this.customizedSQL = customizedSQL;
  }

  @Nullable
  public Object getStart() {
    return start;
  }

  @Nullable
  public Object getEnd() {
    return end;
  }

  @Nullable
  public String getCustomizedSQL() {
    return customizedSQL;
  }

  @Override
  public boolean equals(Object another) {
    if (this == another) {
      return true;
    }
    if (another == null || getClass() != another.getClass()) {
      return false;
    }
    ChunkInfo anotherChunk = (ChunkInfo) another;
    return Objects.equals(start, anotherChunk.start)
        && Objects.equals(end, anotherChunk.end)
        && Objects.equals(customizedSQL, anotherChunk.customizedSQL);
  }

}
