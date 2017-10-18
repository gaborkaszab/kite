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
package org.kitesdk.data.hbase;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetOperationException;
import org.kitesdk.data.RandomAccessDataset;
import org.kitesdk.data.URIBuilder;
import org.kitesdk.data.hbase.avro.GenericAvroDao;
import org.kitesdk.data.hbase.avro.SpecificAvroDao;
import org.kitesdk.data.hbase.impl.Dao;
import org.kitesdk.data.hbase.impl.SchemaManager;
import org.kitesdk.data.hbase.manager.DefaultSchemaManager;
import org.kitesdk.data.spi.AbstractDatasetRepository;

import com.google.common.base.Preconditions;

/**
 * @deprecated Kite DataSet API is deprecated as of CDH6.0.0 and will be removed from CDH in an upcoming release.
 * Cloudera recommends that you use the equivalent API in Spark instead of the Kite DataSet API.
 */
@Deprecated
public class HBaseDatasetRepository extends AbstractDatasetRepository {

  private static final String DEFAULT_NAMESPACE = "default";

  private Connection connection;
  private SchemaManager schemaManager;
  private HBaseMetadataProvider metadataProvider;
  private final URI repositoryUri;

  HBaseDatasetRepository(Admin hBaseAdmin, Connection connection, URI repositoryUri) {
    this.connection = connection;
    this.schemaManager = new DefaultSchemaManager(connection);
    this.metadataProvider = new HBaseMetadataProvider(hBaseAdmin, schemaManager);
    this.repositoryUri = repositoryUri;
  }

  @Override
  public <E> RandomAccessDataset<E> create(String namespace, String name, DatasetDescriptor descriptor, Class<E> type) {
    Preconditions.checkArgument(DEFAULT_NAMESPACE.equals(namespace),
        "Non-default namespaces are not supported");
    Preconditions.checkNotNull(name, "Dataset name cannot be null");
    Preconditions.checkNotNull(descriptor, "Descriptor cannot be null");

    DatasetDescriptor newDescriptor = metadataProvider.create(namespace, name, descriptor);
    return newDataset(namespace, name, newDescriptor, type);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <E> RandomAccessDataset<E> create(String namespace, String name, DatasetDescriptor descriptor) {
    return (RandomAccessDataset<E>) create(namespace, name, descriptor, Object.class);
  }

  @Override
  public <E> RandomAccessDataset<E> update(String namespace, String name, DatasetDescriptor descriptor, Class<E> type) {
    Preconditions.checkArgument(DEFAULT_NAMESPACE.equals(namespace),
        "Non-default namespaces are not supported");
    Preconditions.checkNotNull(name, "Dataset name cannot be null");
    Preconditions.checkNotNull(descriptor, "Descriptor cannot be null");

    DatasetDescriptor newDescriptor = metadataProvider.update(namespace, name, descriptor);
    return newDataset(namespace, name, newDescriptor, type);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <E> RandomAccessDataset<E> update(String namespace, String name, DatasetDescriptor descriptor) {
    return (RandomAccessDataset<E>) update(namespace, name, descriptor, Object.class);
  }

  @Override
  public <E> RandomAccessDataset<E> load(String namespace, String name, Class<E> type) {
    Preconditions.checkArgument(DEFAULT_NAMESPACE.equals(namespace),
        "Non-default namespaces are not supported");
    Preconditions.checkNotNull(name, "Dataset name cannot be null");

    String tableName = HBaseMetadataProvider.getTableName(name).getNameAsString();
    String entityName = HBaseMetadataProvider.getEntityName(name);
    if (entityName.contains(".")) {
      List<DatasetDescriptor> descriptors = new ArrayList<DatasetDescriptor>();
      for (String subEntityName : entityName.split("\\.")) {
        DatasetDescriptor descriptor = metadataProvider.load(namespace, tableName + "." + subEntityName);
        descriptors.add(descriptor);
      }
      return newCompositeDataset(namespace, name, tableName, descriptors, type);
    } else {
      DatasetDescriptor descriptor = metadataProvider.load(namespace, name);
      return newDataset(namespace, name, descriptor, type);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <E> RandomAccessDataset<E> load(String namespace, String name) {
    return (RandomAccessDataset<E>) load(namespace, name, Object.class);
  }

  @Override
  public URI getUri() {
    return repositoryUri;
  }

  @SuppressWarnings("unchecked")
  private <E> RandomAccessDataset<E> newCompositeDataset(String namespace, String name, String tableName,
      List<DatasetDescriptor> descriptors, Class<E> type) {
    List<Class<SpecificRecord>> subEntityClasses = new ArrayList<Class<SpecificRecord>>();
    for (DatasetDescriptor descriptor : descriptors) {
      try {
        Class<SpecificRecord> subEntityClass = (Class<SpecificRecord>) Class
            .forName(descriptor.getSchema().getFullName());
        subEntityClasses.add(subEntityClass);
      } catch (ClassNotFoundException e) {
        throw new DatasetOperationException("Failed to resolve sub-type", e);
      }
    }
    Dao dao = SpecificAvroDao.buildCompositeDaoWithEntityManager(connection,
        tableName, subEntityClasses, schemaManager);
    return new DaoDataset<E>(namespace, name, dao, descriptors.get(0),
        new URIBuilder(repositoryUri, namespace, name).build(), type);
  }

  @SuppressWarnings("unchecked")
  private <E> RandomAccessDataset<E> newDataset(String namespace, String name, DatasetDescriptor descriptor, Class<E> type) {
    // TODO: use descriptor.getFormat() to decide type of DAO (Avro vs. other)
    String tableName = HBaseMetadataProvider.getTableName(name).getNameAsString();
    String entityName = HBaseMetadataProvider.getEntityName(name);
    Dao dao;
    if (isSpecific(descriptor)) {
      dao = new SpecificAvroDao(connection, tableName, entityName, schemaManager);
    } else {
      dao = new GenericAvroDao(connection, tableName, entityName, schemaManager);
    }
    return new DaoDataset(namespace, name, dao, descriptor,
        new URIBuilder(repositoryUri, namespace, name).build(), type);
  }

  private static boolean isSpecific(DatasetDescriptor descriptor) {
    try {
      Class.forName(descriptor.getSchema().getFullName());
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  @Override
  public boolean delete(String namespace, String name) {
    Preconditions.checkArgument(DEFAULT_NAMESPACE.equals(namespace),
        "Non-default namespaces are not supported");
    Preconditions.checkNotNull(name, "Dataset name cannot be null");

    return metadataProvider.delete(namespace, name);
  }

  @Override
  public boolean exists(String namespace, String name) {
    Preconditions.checkArgument(DEFAULT_NAMESPACE.equals(namespace),
        "Non-default namespaces are not supported");
    Preconditions.checkNotNull(name, "Dataset name cannot be null");

    return metadataProvider.exists(namespace, name);
  }

  @Override
  public Collection<String> namespaces() {
    return metadataProvider.namespaces();
  }

  @Override
  public Collection<String> datasets(String namespace) {
    return metadataProvider.datasets(namespace);
  }

  public static class Builder {

    private Configuration configuration;

    public Builder configuration(Configuration configuration) {
      this.configuration = configuration;
      return this;
    }

    public HBaseDatasetRepository build() {
      Connection connection;
      Admin admin;
      try {
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
      } catch (MasterNotRunningException e) {
        throw new DatasetOperationException(
            "Problem creating HBaseDatasetRepository.", e);
      } catch (ZooKeeperConnectionException e) {
        throw new DatasetOperationException(
            "Problem creating HBaseDatasetRepository.", e);
      } catch (IOException e) {
        throw new DatasetIOException(
            "Problem creating HBaseDatasetRepository.", e);
      }
      return new HBaseDatasetRepository(admin, connection, getRepositoryUri(configuration));
    }

    private URI getRepositoryUri(Configuration conf) {
      return URI.create(String.format("repo:hbase:%s:%s",
          conf.get(HConstants.ZOOKEEPER_QUORUM),
          conf.get(HConstants.ZOOKEEPER_CLIENT_PORT)));
    }
  }
}
