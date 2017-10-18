/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.hbase.impl;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.Flushable;
import org.kitesdk.data.spi.AbstractDatasetWriter;
import org.kitesdk.data.spi.ReaderWriterState;

import com.google.common.base.Preconditions;

/**
 * @deprecated Kite DataSet API is deprecated as of CDH6.0.0 and will be removed from CDH in an upcoming release.
 * Cloudera recommends that you use the equivalent API in Spark instead of the Kite DataSet API.
 */
@Deprecated
public class BaseEntityBatch<E> extends AbstractDatasetWriter<E> implements
    EntityBatch<E>, Flushable {
  private final Table table;
  private final EntityMapper<E> entityMapper;
  private final HBaseClientTemplate clientTemplate;
  private final BufferedMutator mutator;
  private ReaderWriterState state;

  /**
   * Checks an Table out of the Connection and modifies it to take advantage of
   * batch puts. This is very useful when performing many consecutive puts.
   *
   * @param clientTemplate
   *          The client template to use
   * @param entityMapper
   *          The EntityMapper to use for mapping
   * @param pool
   *          The HBase table pool
   * @param tableName
   *          The name of the HBase table
   * @param writeBufferSize
   *          Buffer size before flushing writes, in bytes.
   */
  public BaseEntityBatch(HBaseClientTemplate clientTemplate,
      EntityMapper<E> entityMapper, Connection pool, TableName tableName,
      long writeBufferSize) {
    try {
      this.table = pool.getTable(tableName);
      BufferedMutatorParams params = new BufferedMutatorParams(tableName);
      params.writeBufferSize(writeBufferSize);
      this.mutator = pool.getBufferedMutator(params);
    } catch (IOException e) {
      throw new DatasetIOException("Error getting table from connection", e);
    }
    this.clientTemplate = clientTemplate;
    this.entityMapper = entityMapper;
    this.state = ReaderWriterState.NEW;
  }

  /**
   * Checks an Table out of the Connection and modifies it to take advantage of
   * batch puts using the default writeBufferSize (2MB). This is very useful
   * when performing many consecutive puts.
   *
   * @param clientTemplate
   *          The client template to use
   * @param entityMapper
   *          The EntityMapper to use for mapping
   * @param pool
   *          The HBase table pool
   * @param tableName
   *          The name of the HBase table
   */
  public BaseEntityBatch(HBaseClientTemplate clientTemplate,
      EntityMapper<E> entityMapper, Connection pool, TableName tableName) {
    try {
      this.table = pool.getTable(tableName);
      this.mutator = pool.getBufferedMutator(tableName);
    } catch (IOException e) {
      throw new DatasetIOException("Error getting table "
          + tableName.getNameAsString() + " from connection", e);
    }
    this.clientTemplate = clientTemplate;
    this.entityMapper = entityMapper;
    this.state = ReaderWriterState.NEW;
  }

  @Override
  public void initialize() {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
        "Unable to open a writer from state:%s", state);
    state = ReaderWriterState.OPEN;
  }

  @Override
  public void put(E entity) {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to write to a writer in state:%s", state);

    PutAction putAction = entityMapper.mapFromEntity(entity);
    clientTemplate.put(putAction, table);
  }

  @Override
  public void write(E entity) {
    put(entity);
  }

  @Override
  public void flush() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to flush a writer in state:%s", state);

    try {
      mutator.flush();
    } catch (IOException e) {
      throw new DatasetIOException("Error flushing commits for table [" + table
          + "]", e);
    }
  }

  @Override
  public void close() {
    if (state.equals(ReaderWriterState.OPEN)) {
      try {
        mutator.flush();
        mutator.close();
        table.close();
      } catch (IOException e) {
        throw new DatasetIOException("Error closing table [" + table + "]", e);
      }
      state = ReaderWriterState.CLOSED;
    }
  }

  @Override
  public boolean isOpen() {
    return state.equals(ReaderWriterState.OPEN);
  }
}
