///**
// * Copyright 2014 Cloudera Inc.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.kitesdk.data.hbase.tool;
//
//import static org.junit.Assert.assertEquals;
//
//import java.io.File;
//
//import org.apache.commons.io.FileUtils;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.Connection;
//import org.apache.hadoop.hbase.client.ConnectionFactory;
//import org.junit.After;
//import org.junit.AfterClass;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;
//import org.kitesdk.data.hbase.avro.AvroDaoTest;
//import org.kitesdk.data.hbase.avro.AvroUtils;
//import org.kitesdk.data.hbase.avro.SpecificAvroDao;
//import org.kitesdk.data.hbase.avro.entities.SimpleHBaseRecord;
//import org.kitesdk.data.hbase.impl.Dao;
//import org.kitesdk.data.hbase.impl.SchemaManager;
//import org.kitesdk.data.hbase.manager.DefaultSchemaManager;
//import org.kitesdk.data.hbase.testing.HBaseTestUtils;
//import org.kitesdk.data.spi.PartitionKey;
//
///**
// * Test of using schema tool to migrate schemas in a directory.
// */
//public class SchemaToolTest {
//  private static final TableName managedTableName = TableName.valueOf("managed_schemas");
//  private static TableName tableName;
//  private static File simpleSchemaFile;
//  private static String simpleSchema;
//
//  private Connection connection;
//  private SchemaManager manager;
//  private SchemaTool tool;
//
//  @BeforeClass
//  public static void beforeClass() throws Exception {
//    simpleSchema = AvroUtils.inputStreamToString(AvroDaoTest.class
//        .getResourceAsStream("/SchemaTool/SimpleHBaseRecord.avsc"));
//    simpleSchemaFile = FileUtils.toFile(AvroDaoTest.class
//        .getResource("/SchemaTool/SimpleHBaseRecord.avsc"));
//
//    HBaseTestUtils.getMiniCluster();
//  }
//
//  @AfterClass
//  public static void afterClass() throws Exception {
//  }
//
//  @Before
//  public void before() throws Exception {
//    connection = ConnectionFactory.createConnection(HBaseTestUtils.getConf());
//    manager = new DefaultSchemaManager(connection);
//    tool = new SchemaTool(connection.getAdmin(),
//        manager);
//  }
//
//  @After
//  public void after() throws Exception {
//    connection.close();
//    HBaseTestUtils.util.truncateTable(tableName);
//    HBaseTestUtils.util.truncateTable(managedTableName);
//  }
//
//  @Test
//  public void testMigrateDirectory() throws Exception {
//    tool.createOrMigrateSchemaDirectory("classpath:SchemaTool", true);
//
//    // the table name is set in the schema file, so use that table name to
//    // verify that creatOrMigrateSchemaDirectory worked
//    tableName = TableName.valueOf("simple");
//
//    Dao<SimpleHBaseRecord> dao = new SpecificAvroDao<SimpleHBaseRecord>(
//        connection, tableName, "SimpleHBaseRecord", manager);
//    testBasicOperations(dao);
//  }
//
//  @Test
//  public void testMigrateFile() throws Exception {
//    tool.createOrMigrateSchemaFile(tableName.getNameAsString(), simpleSchemaFile, true);
//
//    Dao<SimpleHBaseRecord> dao = new SpecificAvroDao<SimpleHBaseRecord>(
//        connection, tableName, "SimpleHBaseRecord", manager);
//    testBasicOperations(dao);
//  }
//
//  @Test
//  public void testMigrateSchema() throws Exception {
//    tool.createOrMigrateSchema(tableName.getNameAsString(), simpleSchema, true);
//
//    Dao<SimpleHBaseRecord> dao = new SpecificAvroDao<SimpleHBaseRecord>(
//        connection, tableName, "SimpleHBaseRecord", manager);
//    testBasicOperations(dao);
//  }
//
//  private void testBasicOperations(Dao<SimpleHBaseRecord> dao) {
//    SimpleHBaseRecord r1 = SimpleHBaseRecord.newBuilder().setField1("Field 1")
//        .setKeyPart1("KeyPart 1").build();
//    dao.put(r1);
//
//    PartitionKey key = new PartitionKey("KeyPart 1");
//    SimpleHBaseRecord r2 = dao.get(key);
//    assertEquals(r1.getField1().toString(), r2.getField1().toString());
//    assertEquals(r1.getKeyPart1().toString(), r2.getKeyPart1().toString());
//  }
//}
