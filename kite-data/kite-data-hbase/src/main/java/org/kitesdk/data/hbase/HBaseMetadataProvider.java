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
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.hbase.avro.AvroEntitySchema;
import org.kitesdk.data.hbase.impl.Constants;
import org.kitesdk.data.hbase.impl.EntitySchema;
import org.kitesdk.data.hbase.impl.SchemaManager;
import org.kitesdk.data.spi.AbstractMetadataProvider;
import org.kitesdk.data.spi.ColumnMappingParser;
import org.kitesdk.data.spi.Compatibility;
import org.kitesdk.data.spi.PartitionStrategyParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

class HBaseMetadataProvider extends AbstractMetadataProvider {

  private static final Logger LOG = LoggerFactory
      .getLogger(HBaseMetadataProvider.class);

  private static final String DEFAULT_NAMESPACE = "default";
  private static final String REPLICATION_ID_PROP = "hbase.replication.scope";

  private Admin hbaseAdmin;
  private SchemaManager schemaManager;

  public HBaseMetadataProvider(Admin hbaseAdmin, SchemaManager schemaManager) {
    this.hbaseAdmin = hbaseAdmin;
    this.schemaManager = schemaManager;
  }

  @Override
  public DatasetDescriptor create(String namespace, String name, DatasetDescriptor descriptor) {
    Preconditions.checkArgument(DEFAULT_NAMESPACE.equals(namespace),
        "Non-default namespaces are not supported");
    Preconditions.checkNotNull(name, "Dataset name cannot be null");
    Preconditions.checkNotNull(descriptor, "Descriptor cannot be null");
    Compatibility.checkAndWarn(
        namespace,
        HBaseMetadataProvider.getTableName(name).getNameAsString(),
        descriptor.getSchema());
    Preconditions.checkArgument(descriptor.isColumnMapped(),
        "Cannot create dataset %s: missing column mapping", name);

    try {
      TableName managedSchemaName = TableName.valueOf("managed_schemas"); // TODO: allow table to be specified
      if (!hbaseAdmin.tableExists(managedSchemaName)) {
        HTableDescriptor table = new HTableDescriptor(managedSchemaName);
        table.addFamily(new HColumnDescriptor("meta"));
        table.addFamily(new HColumnDescriptor("schema"));
        table.addFamily(new HColumnDescriptor(Constants.SYS_COL_FAMILY));
        hbaseAdmin.createTable(table);
      }
    } catch (IOException e) {
      throw new DatasetIOException("Cannot open schema table", e);
    }

    Schema schema = getEmbeddedSchema(descriptor);
    String entitySchemaString = schema.toString(true);
    AvroEntitySchema entitySchema = new AvroEntitySchema(
        schema, entitySchemaString, descriptor.getColumnMapping());

    TableName tableName = getTableName(name);
    String entityName = getEntityName(name);

    schemaManager.refreshManagedSchemaCache(tableName.getNameAsString(), entityName);
    schemaManager.createSchema(tableName.getNameAsString(), entityName, entitySchemaString,
        "org.kitesdk.data.hbase.avro.AvroKeyEntitySchemaParser",
        "org.kitesdk.data.hbase.avro.AvroKeySerDe",
        "org.kitesdk.data.hbase.avro.AvroEntitySerDe");

    try {
      if (!hbaseAdmin.tableExists(tableName)) {
        HTableDescriptor desc = new HTableDescriptor(tableName);
        Set<String> familiesToAdd = entitySchema.getColumnMappingDescriptor()
            .getRequiredColumnFamilies();
        familiesToAdd.add(new String(Constants.SYS_COL_FAMILY));
        familiesToAdd.add(new String(Constants.OBSERVABLE_COL_FAMILY));
        for (String columnFamily : familiesToAdd) {
          desc.addFamily(columnFamily(columnFamily, descriptor));
        }
        hbaseAdmin.createTable(desc);
      } else {
        Set<String> familiesToAdd = entitySchema.getColumnMappingDescriptor()
            .getRequiredColumnFamilies();
        familiesToAdd.add(new String(Constants.SYS_COL_FAMILY));
        familiesToAdd.add(new String(Constants.OBSERVABLE_COL_FAMILY));
        HTableDescriptor desc = hbaseAdmin.getTableDescriptor(tableName);
        for (HColumnDescriptor columnDesc : desc.getColumnFamilies()) {
          String familyName = columnDesc.getNameAsString();
          if (familiesToAdd.contains(familyName)) {
            familiesToAdd.remove(familyName);
          }
        }
        if (familiesToAdd.size() > 0) {
          hbaseAdmin.disableTable(tableName);
          try {
            for (String family : familiesToAdd) {
              hbaseAdmin.addColumn(tableName, columnFamily(family, descriptor));
            }
          } finally {
            hbaseAdmin.enableTable(tableName);
          }
        }
      }
    } catch (IOException e) {
      throw new DatasetIOException("Cannot prepare table: " + name, e);
    }
    return getDatasetDescriptor(schema, descriptor.getLocation());
  }

  @Override
  public DatasetDescriptor update(String namespace, String name, DatasetDescriptor descriptor) {
    Preconditions.checkArgument(DEFAULT_NAMESPACE.equals(namespace),
        "Non-default namespaces are not supported");
    Preconditions.checkNotNull(name, "Dataset name cannot be null");
    Preconditions.checkNotNull(descriptor, "Descriptor cannot be null");
    Compatibility.checkAndWarn(
        namespace,
        HBaseMetadataProvider.getTableName(name).getNameAsString(),
        descriptor.getSchema());
    Preconditions.checkArgument(descriptor.isColumnMapped(),
        "Cannot update dataset %s: missing column mapping", name);

    TableName tableName = getTableName(name);
    String entityName = getEntityName(name);
    schemaManager.refreshManagedSchemaCache(tableName.getNameAsString(), entityName);

    Schema newSchema = getEmbeddedSchema(descriptor);

    String schemaString = newSchema.toString(true);
    EntitySchema entitySchema = new AvroEntitySchema(
        newSchema, schemaString, descriptor.getColumnMapping());

    if (!schemaManager.hasSchemaVersion(tableName.getNameAsString(), entityName, entitySchema)) {
      schemaManager.migrateSchema(tableName.getNameAsString(), entityName, schemaString);
    } else {
      LOG.info("Schema hasn't changed, not migrating: (" + name + ")");
    }
    return getDatasetDescriptor(newSchema, descriptor.getLocation());
  }

  @Override
  public DatasetDescriptor load(String namespace, String name) {
    Preconditions.checkArgument(DEFAULT_NAMESPACE.equals(namespace),
        "Non-default namespaces are not supported");
    Preconditions.checkNotNull(name, "Dataset name cannot be null");

    if (!exists(namespace, name)) {
      throw new DatasetNotFoundException("No such dataset: " + name);
    }
    TableName tableName = getTableName(name);
    String entityName = getEntityName(name);
    return new DatasetDescriptor.Builder()
        .schemaLiteral(schemaManager.getEntitySchema(tableName.getNameAsString(), entityName)
            .getRawSchema())
        .build();
  }

  @Override
  public boolean delete(String namespace, String name) {
    Preconditions.checkArgument(DEFAULT_NAMESPACE.equals(namespace),
        "Non-default namespaces are not supported");
    Preconditions.checkNotNull(name, "Dataset name cannot be null");

    DatasetDescriptor descriptor;
    try {
      descriptor = load(namespace, name);
    } catch (DatasetNotFoundException e) {
      return false;
    }
    Preconditions.checkState(descriptor.isColumnMapped(),
        "[BUG] Existing descriptor has no column mapping");

    TableName tableName = getTableName(name);
    String entityName = getEntityName(name);

    schemaManager.deleteSchema(tableName.getNameAsString(), entityName);

    // TODO: this may delete columns for other entities if they share column families
    // TODO: https://issues.cloudera.org/browse/CDK-145, https://issues.cloudera.org/browse/CDK-146
    for (String columnFamily : descriptor.getColumnMapping().getRequiredColumnFamilies()) {
      try {
        hbaseAdmin.disableTable(tableName);
        try {
          hbaseAdmin.deleteColumn(tableName, Bytes.toBytes(columnFamily));
        } finally {
          hbaseAdmin.enableTable(tableName);
        }
      } catch (IOException e) {
        throw new DatasetIOException("Cannot delete " + name, e);
      }
    }
    return true;
  }

  @Override
  public boolean exists(String namespace, String name) {
    Preconditions.checkArgument(DEFAULT_NAMESPACE.equals(namespace),
        "Non-default namespaces are not supported");
    Preconditions.checkNotNull(name, "Dataset name cannot be null");

    TableName tableName = getTableName(name);
    String entityName = getEntityName(name);
    schemaManager.refreshManagedSchemaCache(tableName.getNameAsString(), entityName);
    return schemaManager.hasManagedSchema(tableName.getNameAsString(), entityName);
  }

  @Override
  public Collection<String> namespaces() {
    return ImmutableList.of(DEFAULT_NAMESPACE);
  }

  @Override
  public Collection<String> datasets(String namespace) {
    List<String> datasets = Lists.newArrayList();
    for (String table : schemaManager.getTableNames()) {
      for (String entity : schemaManager.getEntityNames(table)) {
        datasets.add(table + "." + entity);
      }
    }
    return datasets;
  }

  static TableName getTableName(String name) {
    // TODO: change to use namespace (CDK-140)
    if (name.contains(".")) {
      return TableName.valueOf(name.substring(0, name.indexOf('.')));
    }
    return TableName.valueOf(name);
  }

  static String getEntityName(String name) {
    return name.substring(name.indexOf('.') + 1);
  }

  private static Schema getEmbeddedSchema(DatasetDescriptor descriptor) {
    // the SchemaManager stores schemas, so this embeds the column mapping and
    // partition strategy in the schema. the result is parsed by
    // AvroKeyEntitySchemaParser
    Schema schema = descriptor.getSchema();
    if (descriptor.isColumnMapped()) {
      schema = ColumnMappingParser
          .embedColumnMapping(schema, descriptor.getColumnMapping());
    }
    if (descriptor.isPartitioned()) {
      schema = PartitionStrategyParser
          .embedPartitionStrategy(schema, descriptor.getPartitionStrategy());
    }
    return schema;
  }

  private static DatasetDescriptor getDatasetDescriptor(Schema schema, URI location) {
    return new DatasetDescriptor.Builder()
        .schema(schema)
        .location(location)
        .build();
  }

  private HColumnDescriptor columnFamily(byte[] family, DatasetDescriptor descriptor) {
    return configure(new HColumnDescriptor(family), descriptor);
  }

  private HColumnDescriptor columnFamily(String family, DatasetDescriptor descriptor) {
    return configure(new HColumnDescriptor(family), descriptor);
  }

  private HColumnDescriptor configure(HColumnDescriptor column, DatasetDescriptor descriptor) {
    if (descriptor.hasProperty(REPLICATION_ID_PROP)) {
      String value = descriptor.getProperty(REPLICATION_ID_PROP);
      try {
        column.setScope(Integer.valueOf(value));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Invalid replication scope: " + value, e);
      }
    }
    return column;
  }

}
