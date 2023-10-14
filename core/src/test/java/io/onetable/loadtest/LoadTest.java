/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package io.onetable.loadtest;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieArchivalConfig;

import io.onetable.TestJavaHudiTable;
import io.onetable.client.OneTableClient;
import io.onetable.client.PerTableConfig;
import io.onetable.client.SourceClientProvider;
import io.onetable.hudi.HudiSourceClientProvider;
import io.onetable.model.storage.TableFormat;
import io.onetable.model.sync.SyncMode;

/**
 * Tests that can be run manually to simulate lots of commits/partitions/files/etc. to understand
 * how the system behaves under these conditions.
 */
@Disabled
public class LoadTest {
  @TempDir public static Path tempDir;
  private static JavaSparkContext jsc;
  private static SparkSession sparkSession;
  private SourceClientProvider<HoodieInstant> hudiSourceClientProvider;

  @BeforeAll
  public static void setupOnce() {
    SparkConf sparkConf =
        new SparkConf()
            .setAppName("onetable-testing")
            .set("spark.serializer", KryoSerializer.class.getName())
            .set("spark.sql.catalog.default_iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .set("spark.sql.catalog.default_iceberg.type", "hadoop")
            .set("spark.sql.catalog.default_iceberg.warehouse", tempDir.toString())
            .set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
            .set("spark.sql.catalog.hadoop_prod.type", "hadoop")
            .set("spark.sql.catalog.hadoop_prod.warehouse", tempDir.toString())
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("parquet.avro.write-old-list-structure", "false")
            // Needed for ignoring not nullable constraints on nested columns in Delta.
            .set("spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled", "true")
            .set("spark.sql.shuffle.partitions", "4")
            .set("spark.default.parallelism", "4")
            .set("spark.sql.session.timeZone", "UTC")
            .set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
            .setMaster("local[4]");
    sparkSession =
        SparkSession.builder().config(HoodieReadClient.addHoodieSupport(sparkConf)).getOrCreate();
    sparkSession
        .sparkContext()
        .hadoopConfiguration()
        .set("parquet.avro.write-old-list-structure", "false");
    jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
  }

  @BeforeEach
  public void setup() {
    hudiSourceClientProvider = new HudiSourceClientProvider();
    hudiSourceClientProvider.init(jsc.hadoopConfiguration(), Collections.emptyMap());
  }

  @Test
  void testFullSyncWithManyPartitions() {
    Path tempDir = Paths.get("/Users/timbrown/code/onetable-oss/onetable/");

    String tableName = "full_sync_many_partitions";
    int numPartitions = 1000;
    int numFilesPerPartition = 100;
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, "level:SIMPLE", HoodieTableType.COPY_ON_WRITE)) {
//      for (int i = 0; i < numFilesPerPartition; i++) {
//        table.insertRecords(
//            1,
//            IntStream.range(0, numPartitions)
//                .mapToObj(partitionNumber -> "partition" + partitionNumber)
//                .collect(Collectors.toList()),
//            false);
//      }
      PerTableConfig perTableConfig =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(Arrays.asList(TableFormat.ICEBERG, TableFormat.DELTA))
              .tableBasePath(table.getBasePath())
              .syncMode(SyncMode.FULL)
              .build();
      OneTableClient oneTableClient = new OneTableClient(jsc.hadoopConfiguration());
      long start = System.currentTimeMillis();
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      long end = System.currentTimeMillis();
      // baseline 137739ms
      System.out.println("Full sync took " + (end - start) + "ms");
    }
  }

  @Test
  void testIncrementalSyncWithManyCommits() throws Exception {
    String tableName = "incremental_sync_many_commmits";

    Path tempDir = Paths.get("/Users/timbrown/code/onetable-oss/onetable/");
    // creates a single file per partition per commit
    int numPartitionsUpdatedPerCommit = 1000;
    int numCommits = 100;
    HoodieArchivalConfig archivalConfig =
        HoodieArchivalConfig.newBuilder()
            .archiveCommitsWith(numCommits + 1, numCommits + 10)
            .build();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, "level:SIMPLE", HoodieTableType.COPY_ON_WRITE, archivalConfig)) {
      //table.insertRecords(1, "partition0", false);
      PerTableConfig perTableConfig =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(Arrays.asList(TableFormat.ICEBERG, TableFormat.DELTA))
              .tableBasePath(table.getBasePath())
              .syncMode(SyncMode.INCREMENTAL)
              .build();
      // sync once to establish first commit
      OneTableClient oneTableClient = new OneTableClient(jsc.hadoopConfiguration());
      //oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
//      for (int i = 0; i < numCommits; i++) {
//        table.insertRecords(
//            1,
//            IntStream.range(0, numPartitionsUpdatedPerCommit)
//                .mapToObj(partitionNumber -> "partition" + partitionNumber)
//                .collect(Collectors.toList()),
//            false);
//      }

      long start = System.currentTimeMillis();
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      long end = System.currentTimeMillis();
      System.out.println("Incremental sync took " + (end - start) + "ms");
    }
  }
}
