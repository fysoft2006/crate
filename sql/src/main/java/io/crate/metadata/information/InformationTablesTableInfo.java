/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.metadata.information;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.planner.RowGranularity;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class InformationTablesTableInfo extends InformationTableInfo {

    public static final String NAME = "tables";
    public static final TableIdent IDENT = new TableIdent(InformationSchemaInfo.NAME, NAME);

    public static class Columns {
        public static final ColumnIdent SCHEMA_NAME = new ColumnIdent("schema_name");
        public static final ColumnIdent TABLE_NAME = new ColumnIdent("table_name");
        public static final ColumnIdent NUMBER_OF_SHARDS = new ColumnIdent("number_of_shards");
        public static final ColumnIdent NUMBER_OF_REPLICAS = new ColumnIdent("number_of_replicas");
        public static final ColumnIdent CLUSTERED_BY = new ColumnIdent("clustered_by");
        public static final ColumnIdent PARTITIONED_BY = new ColumnIdent("partitioned_by");
        public static final ColumnIdent BLOBS_PATH = new ColumnIdent("blobs_path");

        public static final ColumnIdent TABLE_SETTINGS = new ColumnIdent("settings");

        public static final ColumnIdent TABLE_SETTINGS_BLOCKS = new ColumnIdent("settings",
                ImmutableList.of("blocks"));
        public static final ColumnIdent TABLE_SETTINGS_BLOCKS_READ_ONLY = new ColumnIdent("settings",
                ImmutableList.of("blocks", "read_only"));
        public static final ColumnIdent TABLE_SETTINGS_BLOCKS_READ = new ColumnIdent("settings",
                ImmutableList.of("blocks", "read"));
        public static final ColumnIdent TABLE_SETTINGS_BLOCKS_WRITE = new ColumnIdent("settings",
                ImmutableList.of("blocks", "write"));
        public static final ColumnIdent TABLE_SETTINGS_BLOCKS_METADATA = new ColumnIdent("settings",
                ImmutableList.of("blocks", "metadata"));

        public static final ColumnIdent TABLE_SETTINGS_TRANSLOG = new ColumnIdent("settings",
                ImmutableList.of("translog"));
        public static final ColumnIdent TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_OPS = new ColumnIdent("settings",
                ImmutableList.of("translog", "flush_threshold_ops"));
        public static final ColumnIdent TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE = new ColumnIdent("settings",
                ImmutableList.of("translog", "flush_threshold_size"));
        public static final ColumnIdent TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_PERIOD = new ColumnIdent("settings",
                ImmutableList.of("translog", "flush_threshold_period"));
        public static final ColumnIdent TABLE_SETTINGS_TRANSLOG_DISABLE_FLUSH = new ColumnIdent("settings",
                ImmutableList.of("translog", "disable_flush"));
    }

    public static class ReferenceInfos {
        public static final ReferenceInfo SCHEMA_NAME = info(Columns.SCHEMA_NAME, DataTypes.STRING);
        public static final ReferenceInfo TABLE_NAME = info(Columns.TABLE_NAME, DataTypes.STRING);
        public static final ReferenceInfo NUMBER_OF_SHARDS = info(Columns.NUMBER_OF_SHARDS, DataTypes.INTEGER);
        public static final ReferenceInfo NUMBER_OF_REPLICAS = info(Columns.NUMBER_OF_REPLICAS, DataTypes.STRING);
        public static final ReferenceInfo CLUSTERED_BY = info(Columns.CLUSTERED_BY, DataTypes.STRING);
        public static final ReferenceInfo PARTITIONED_BY = info(Columns.PARTITIONED_BY, new ArrayType(DataTypes.STRING));
        public static final ReferenceInfo BLOBS_PATH = info(Columns.BLOBS_PATH, DataTypes.STRING);

        public static final ReferenceInfo TABLE_SETTINGS = info(Columns.TABLE_SETTINGS, DataTypes.OBJECT);

        public static final ReferenceInfo TABLE_SETTINGS_BLOCKS = info(
                Columns.TABLE_SETTINGS_BLOCKS, DataTypes.OBJECT);
        public static final ReferenceInfo TABLE_SETTINGS_BLOCKS_READ_ONLY = info(
                Columns.TABLE_SETTINGS_BLOCKS_READ_ONLY, DataTypes.BOOLEAN);
        public static final ReferenceInfo TABLE_SETTINGS_BLOCKS_READ = info(
                Columns.TABLE_SETTINGS_BLOCKS_READ, DataTypes.BOOLEAN);
        public static final ReferenceInfo TABLE_SETTINGS_BLOCKS_WRITE = info(
                Columns.TABLE_SETTINGS_BLOCKS_WRITE, DataTypes.BOOLEAN);
        public static final ReferenceInfo TABLE_SETTINGS_BLOCKS_METADATA = info(
                Columns.TABLE_SETTINGS_BLOCKS_METADATA, DataTypes.BOOLEAN);

        public static final ReferenceInfo TABLE_SETTINGS_TRANSLOG = info(
                Columns.TABLE_SETTINGS_TRANSLOG, DataTypes.OBJECT);
        public static final ReferenceInfo TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_OPS = info(
                Columns.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_OPS, DataTypes.INTEGER);
        public static final ReferenceInfo TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE = info(
                Columns.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE, DataTypes.LONG);
        public static final ReferenceInfo TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_PERIOD = info(
                Columns.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_PERIOD, DataTypes.LONG);
        public static final ReferenceInfo TABLE_SETTINGS_TRANSLOG_DISABLE_FLUSH = info(
                Columns.TABLE_SETTINGS_TRANSLOG_DISABLE_FLUSH, DataTypes.BOOLEAN);
    }

    private static ReferenceInfo info(ColumnIdent columnIdent, DataType dataType) {
        return new ReferenceInfo(new ReferenceIdent(IDENT, columnIdent), RowGranularity.DOC, dataType);
    }

    public InformationTablesTableInfo(InformationSchemaInfo schemaInfo) {
        super(schemaInfo,
                IDENT,
                ImmutableList.of(Columns.SCHEMA_NAME, Columns.TABLE_NAME),
                ImmutableMap.<ColumnIdent, ReferenceInfo>builder()
                        .put(Columns.SCHEMA_NAME, ReferenceInfos.SCHEMA_NAME)
                        .put(Columns.TABLE_NAME, ReferenceInfos.TABLE_NAME)
                        .put(Columns.NUMBER_OF_SHARDS, ReferenceInfos.NUMBER_OF_SHARDS)
                        .put(Columns.NUMBER_OF_REPLICAS, ReferenceInfos.NUMBER_OF_REPLICAS)
                        .put(Columns.CLUSTERED_BY, ReferenceInfos.CLUSTERED_BY)
                        .put(Columns.PARTITIONED_BY, ReferenceInfos.PARTITIONED_BY)
                        .put(Columns.BLOBS_PATH, ReferenceInfos.BLOBS_PATH)
                        .put(Columns.TABLE_SETTINGS, ReferenceInfos.TABLE_SETTINGS)
                        .build(),
                ImmutableList.<ReferenceInfo>builder()
                        .add(ReferenceInfos.SCHEMA_NAME)
                        .add(ReferenceInfos.TABLE_NAME)
                        .add(ReferenceInfos.NUMBER_OF_SHARDS)
                        .add(ReferenceInfos.NUMBER_OF_REPLICAS)
                        .add(ReferenceInfos.CLUSTERED_BY)
                        .add(ReferenceInfos.PARTITIONED_BY)
                        .add(ReferenceInfos.BLOBS_PATH)
                        .add(ReferenceInfos.TABLE_SETTINGS)
                        .build()
        );
    }
}
