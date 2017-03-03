/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.morphline.solr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.ManagedIndexSchemaFactory;
import org.junit.Ignore;
import org.junit.Test;
import org.kitesdk.morphline.api.MorphlineContext;

/** Verify that the correct Solr Server is selected based on parameters given the locator */
public class SolrLocatorTest {

  private static final String RESOURCES_DIR = "target" + File.separator + "test-classes";
  
  @Test
  public void testSelectsEmbeddedSolrServerAndAddDocument() throws Exception {
    //Solr locator should select EmbeddedSolrServer only solrHome is specified
    SolrLocator solrLocator = new SolrLocator(new SolrMorphlineContext.Builder().build());
    solrLocator.setSolrHomeDir(RESOURCES_DIR + "/solr");
    solrLocator.setCollectionName("collection1");

    SolrServerDocumentLoader documentLoader = (SolrServerDocumentLoader)solrLocator.getLoader();
    SolrClient solrServer = documentLoader.getSolrServer();
    
    assertTrue(solrServer instanceof EmbeddedSolrServer);
    
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "myId");
    doc.addField("text", "myValue");
    solrServer.add(doc);
    solrServer.commit();
    
    SolrDocument resultDoc = solrServer.getById("myId");
    assertTrue(resultDoc.getFieldValues("text").contains("myValue"));
    
    UpdateResponse deleteResponse = solrServer.deleteById("myId");
    assertEquals(0, deleteResponse.getStatus());
    solrServer.commit();
    solrServer.close();
  }
  
  @Test
  public void testIndexSchemaCreation() {
    //Solr locator should select EmbeddedSolrServer only solrHome is specified
    SolrLocator solrLocator = new SolrLocator(new SolrMorphlineContext.Builder().build());
    solrLocator.setSolrHomeDir(RESOURCES_DIR + "/solr/collection1");
    solrLocator.setCollectionName("collection1");
    
    IndexSchema indexSchema = solrLocator.getIndexSchema();
    
    assertNotNull(indexSchema);

    assertEquals("example", indexSchema.getSchemaName());
    assertEquals("schema.xml", indexSchema.getResourceName());
  }

  @Test
  public void testManagedIndexSchemaCreation() {
    //Solr locator should select EmbeddedSolrServer only solrHome is specified
    SolrLocator solrLocator = new SolrLocator(new SolrMorphlineContext.Builder().build());
    solrLocator.setSolrHomeDir(RESOURCES_DIR + "/solr/managedSchemaCollection");
    solrLocator.setCollectionName("example-managed");

    IndexSchema indexSchema = solrLocator.getIndexSchema();

    assertNotNull(indexSchema);
    assertTrue(indexSchema instanceof ManagedIndexSchema);
    
    assertEquals("example-managed", indexSchema.getSchemaName());
  }

}
