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
 
package io.onetable.delta;

import static io.onetable.delta.DeltaValueSerializer.getFormattedValueForColumnStats;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.NumericType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.stat.ColumnStat;

/**
 * DeltaStatsExtractor extracts column stats and also responsible for their serialization leveraging
 * {@link DeltaValueSerializer}.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DeltaStatsExtractor {
  private static final DeltaStatsExtractor INSTANCE = new DeltaStatsExtractor();
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String NUM_RECORDS = "numRecords";
  private static final String MIN_VALUES = "minValues";
  private static final String MAX_VALUES = "maxValues";
  private static final String NULL_COUNT = "nullCount";

  public static DeltaStatsExtractor getInstance() {
    return INSTANCE;
  }

  public String convertStatsToDeltaFormat(
      StructType deltaTableSchema, long numRecords, Map<OneField, ColumnStat> columnStats)
      throws JsonProcessingException {
    Map<String, Object> deltaStatsObject = new HashMap<>(4, 1.0f);
    deltaStatsObject.put(NUM_RECORDS, numRecords);
    if (columnStats == null) {
      return deltaStatsObject.toString();
    }
    Set<String> validPaths = getPathsFromStructSchemaForMinAndMaxStats(deltaTableSchema, "");
    Map<OneField, ColumnStat> validColumnStats = columnStats.entrySet().stream()
        .filter(e -> validPaths.contains(e.getKey().getPath()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    deltaStatsObject.put(
        MIN_VALUES, getMinValues(validColumnStats));
    deltaStatsObject.put(
        MAX_VALUES, getMaxValues(validColumnStats));
    deltaStatsObject.put(NULL_COUNT, getNullCount(validColumnStats));
    return MAPPER.writeValueAsString(deltaStatsObject);
  }

  private Set<String> getPathsFromStructSchemaForMinAndMaxStats(
      StructType schema, String parentPath) {
    Set<String> allPaths = new HashSet<>();
    for (StructField sf : schema.fields()) {
      // Delta only supports min/max stats for these fields.
      if (sf.dataType() instanceof DateType
          || sf.dataType() instanceof NumericType
          || sf.dataType() instanceof TimestampType
          || sf.dataType() instanceof StringType) {
        allPaths.add(combinePath(parentPath, sf.name()));
      } else if (sf.dataType() instanceof StructType) {
        allPaths.addAll(
            getPathsFromStructSchemaForMinAndMaxStats(
                (StructType) sf.dataType(), combinePath(parentPath, sf.name())));
      }
    }
    return allPaths;
  }

  private String combinePath(String parentPath, String fieldName) {
    if (parentPath == null || parentPath.isEmpty()) {
      return fieldName;
    }
    return parentPath + "." + fieldName;
  }

  private Map<String, Object> getMinValues(
      Map<OneField, ColumnStat> validColumnStats) {
    return getValues(
        validColumnStats,
        columnStat -> columnStat.getRange().getMinValue());
  }

  private Map<String, Object> getMaxValues(
      Map<OneField, ColumnStat> validColumnStats) {
    return getValues(
        validColumnStats,
        columnStat -> columnStat.getRange().getMaxValue());
  }

  private Map<String, Object> getValues(
      Map<OneField, ColumnStat> validColumnStats,
      Function<ColumnStat, Object> valueExtractor) {
    Map<String, Object> jsonObject = new HashMap<>();
    validColumnStats.forEach(
        (field, columnStat) -> {
          String[] pathParts = field.getPathParts();
          insertValueAtPath(
              jsonObject,
              pathParts,
              getFormattedValueForColumnStats(valueExtractor.apply(columnStat), field.getSchema()));
        });
    return jsonObject;
  }

  private Map<String, Object> getNullCount(Map<OneField, ColumnStat> validColumnStats) {
    // TODO: Additional work needed to track nulls maps & arrays.
    Map<String, Object> jsonObject = new HashMap<>();
    validColumnStats.forEach(
        (field, columnStat) -> {
          String[] pathParts = field.getPathParts();
          insertValueAtPath(
              jsonObject,
              pathParts,
              columnStat.getNumNulls());
        });
    return jsonObject;
  }

  private void insertValueAtPath(Map<String, Object> jsonObject, String[] pathParts, Object value) {
    if (pathParts == null || pathParts.length == 0) {
      return;
    }
    Map<String, Object> currObject = jsonObject;
    for (int i = 0; i < pathParts.length; i++) {
      String part = pathParts[i];
      if (i == pathParts.length - 1) {
        currObject.put(part, value);
      } else {
        if (!currObject.containsKey(part)) {
          currObject.put(part, new HashMap<String, Object>());
        }
        try {
          currObject = (HashMap<String, Object>) currObject.get(part);
        } catch (ClassCastException e) {
          throw new RuntimeException(
              String.format(
                  "Cannot cast to hashmap while inserting stats at path %s",
                  String.join("->", pathParts)),
              e);
        }
      }
    }
  }

  private Map<String, ColumnStat> getColumnStatKeyedByFullyQualifiedPath(
      Map<OneField, ColumnStat> columnStats) {
    return columnStats.entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey().getPath(), Map.Entry::getValue));
  }

  private Map<String, OneField> getPathToFieldMap(Map<OneField, ColumnStat> columnStats) {
    return columnStats.entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey().getPath(), Map.Entry::getKey));
  }
}
