/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.metadata.doc;

import com.google.common.base.MoreObjects;
import com.google.common.collect.*;
import io.crate.Constants;
import io.crate.analyze.TableParameter;
import io.crate.analyze.TableParameterInfo;
import io.crate.core.NumberOfReplicas;
import io.crate.exceptions.TableAliasSchemaException;
import io.crate.metadata.*;
import io.crate.metadata.settings.CrateTableSettings;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.planner.RowGranularity;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;

import java.io.IOException;
import java.util.*;

public class DocIndexMetaData {

    private static final String ID = "_id";
    public static final ColumnIdent ID_IDENT = new ColumnIdent(ID);
    private final IndexMetaData metaData;

    private final MappingMetaData defaultMappingMetaData;
    private final Map<String, Object> defaultMappingMap;

    private final Map<ColumnIdent, IndexReferenceInfo.Builder> indicesBuilder = new HashMap<>();

    private final ImmutableSortedSet.Builder<ReferenceInfo> columnsBuilder = ImmutableSortedSet.orderedBy(new Comparator<ReferenceInfo>() {
        @Override
        public int compare(ReferenceInfo o1, ReferenceInfo o2) {
            return o1.ident().columnIdent().fqn().compareTo(o2.ident().columnIdent().fqn());
        }
    });

    // columns should be ordered
    private final ImmutableMap.Builder<ColumnIdent, ReferenceInfo> referencesBuilder = ImmutableSortedMap.naturalOrder();
    private final ImmutableList.Builder<ReferenceInfo> partitionedByColumnsBuilder = ImmutableList.builder();

    private final TableIdent ident;
    private final int numberOfShards;
    private final BytesRef numberOfReplicas;
    private final ImmutableMap<String, Object> tableParameters;
    private Map<String, Object> metaMap;
    private Map<String, Object> metaColumnsMap;
    private Map<String, Object> indicesMap;
    private List<List<String>> partitionedByList;
    private ImmutableList<ReferenceInfo> columns;
    private ImmutableMap<ColumnIdent, IndexReferenceInfo> indices;
    private ImmutableList<ReferenceInfo> partitionedByColumns;
    private ImmutableMap<ColumnIdent, ReferenceInfo> references;
    private ImmutableList<ColumnIdent> primaryKey;
    private ColumnIdent routingCol;
    private ImmutableList<ColumnIdent> partitionedBy;
    private final boolean isAlias;
    private final Set<String> aliases;
    private boolean hasAutoGeneratedPrimaryKey = false;

    private ColumnPolicy columnPolicy = ColumnPolicy.DYNAMIC;

    private final static ImmutableMap<String, DataType> dataTypeMap = ImmutableMap.<String, DataType>builder()
            .put("date", DataTypes.TIMESTAMP)
            .put("string", DataTypes.STRING)
            .put("boolean", DataTypes.BOOLEAN)
            .put("byte", DataTypes.BYTE)
            .put("short", DataTypes.SHORT)
            .put("integer", DataTypes.INTEGER)
            .put("long", DataTypes.LONG)
            .put("float", DataTypes.FLOAT)
            .put("double", DataTypes.DOUBLE)
            .put("ip", DataTypes.IP)
            .put("geo_point", DataTypes.GEO_POINT)
            .put("object", DataTypes.OBJECT)
            .put("nested", DataTypes.OBJECT).build();

    public DocIndexMetaData(IndexMetaData metaData, TableIdent ident) throws IOException {
        this.ident = ident;
        this.metaData = metaData;
        this.isAlias = !metaData.getIndex().equals(ident.esName());
        this.numberOfShards = metaData.numberOfShards();
        final Settings settings = metaData.getSettings();
        this.numberOfReplicas = NumberOfReplicas.fromSettings(settings);
        this.aliases = ImmutableSet.copyOf(metaData.aliases().keys().toArray(String.class));
        this.defaultMappingMetaData = this.metaData.mappingOrDefault(Constants.DEFAULT_MAPPING_TYPE);
        if (defaultMappingMetaData == null) {
            this.defaultMappingMap = new HashMap<>();
        } else {
            this.defaultMappingMap = this.defaultMappingMetaData.sourceAsMap();
        }
        this.tableParameters = ImmutableMap.<String,Object>builder()
                .put(TableParameterInfo.READ_ONLY,
                        settings.getAsBoolean(TableParameterInfo.READ_ONLY, CrateTableSettings.READ_ONLY.defaultValue()))
                .put(TableParameterInfo.BLOCKS_READ,
                        settings.getAsBoolean(TableParameterInfo.BLOCKS_READ, CrateTableSettings.BLOCKS_READ.defaultValue()))
                .put(TableParameterInfo.BLOCKS_WRITE,
                        settings.getAsBoolean(TableParameterInfo.BLOCKS_WRITE, CrateTableSettings.BLOCKS_WRITE.defaultValue()))
                .put(TableParameterInfo.BLOCKS_METADATA,
                        settings.getAsBoolean(TableParameterInfo.BLOCKS_METADATA, CrateTableSettings.BLOCKS_METADATA.defaultValue()))
                .put(TableParameterInfo.FLUSH_THRESHOLD_OPS,
                        settings.getAsInt(TableParameterInfo.FLUSH_THRESHOLD_OPS, CrateTableSettings.FLUSH_THRESHOLD_OPS.defaultValue()))
                .put(TableParameterInfo.FLUSH_THRESHOLD_PERIOD,
                        settings.getAsTime(TableParameterInfo.FLUSH_THRESHOLD_PERIOD, CrateTableSettings.FLUSH_THRESHOLD_PERIOD.defaultValue()))
                .put(TableParameterInfo.FLUSH_THRESHOLD_SIZE,
                        settings.getAsBytesSize(TableParameterInfo.FLUSH_THRESHOLD_SIZE, CrateTableSettings.FLUSH_THRESHOLD_SIZE.defaultValue()))
                .put(TableParameterInfo.FLUSH_DISABLE,
                        settings.getAsBoolean(TableParameterInfo.FLUSH_DISABLE, CrateTableSettings.FLUSH_DISABLE.defaultValue()))
                .put(TableParameterInfo.TRANSLOG_INTERVAL,
                        settings.getAsTime(TableParameterInfo.TRANSLOG_INTERVAL, CrateTableSettings.TRANSLOG_INTERVAL.defaultValue()))
                .put(TableParameterInfo.ROUTING_ALLOCATION_ENABLE,
                        settings.get(TableParameterInfo.ROUTING_ALLOCATION_ENABLE, CrateTableSettings.ROUTING_ALLOCATION_ENABLE.defaultValue()))
                .put(TableParameterInfo.TOTAL_SHARDS_PER_NODE,
                        settings.getAsInt(TableParameterInfo.TOTAL_SHARDS_PER_NODE, CrateTableSettings.TOTAL_SHARDS_PER_NODE.defaultValue()))
                .put(TableParameterInfo.RECOVERY_INITIAL_SHARDS,
                        settings.get(TableParameterInfo.RECOVERY_INITIAL_SHARDS, CrateTableSettings.RECOVERY_INITIAL_SHARDS.defaultValue()))
                .put(TableParameterInfo.WARMER_ENABLED,
                        settings.getAsBoolean(TableParameterInfo.WARMER_ENABLED, CrateTableSettings.WARMER_ENABLED.defaultValue()))
                .build();

        prepareCrateMeta();
    }

    @SuppressWarnings("unchecked")
    private static <T> T getNested(Map map, String key) {
        return (T) map.get(key);
    }

    private void prepareCrateMeta() {
        metaMap = getNested(defaultMappingMap, "_meta");
        if (metaMap != null) {
            indicesMap = getNested(metaMap, "indices");
            if (indicesMap == null) {
                indicesMap = ImmutableMap.of();
            }
            metaColumnsMap = getNested(metaMap, "columns");
            if (metaColumnsMap == null) {
                metaColumnsMap = ImmutableMap.of();
            }

            partitionedByList = getNested(metaMap, "partitioned_by");
            if (partitionedByList == null) {
                partitionedByList = ImmutableList.of();
            }
        } else {
            metaMap = new HashMap<>();
            indicesMap = new HashMap<>();
            metaColumnsMap = new HashMap<>();
            partitionedByList = ImmutableList.of();
        }
    }

    private void addPartitioned(ColumnIdent column, DataType type) {
        add(column, type, ColumnPolicy.DYNAMIC, ReferenceInfo.IndexType.NOT_ANALYZED, true);
    }

    private void add(ColumnIdent column, DataType type, ReferenceInfo.IndexType indexType) {
        add(column, type, ColumnPolicy.DYNAMIC, indexType, false);
    }

    private void add(ColumnIdent column, DataType type, ColumnPolicy columnPolicy,
                     ReferenceInfo.IndexType indexType, boolean partitioned) {
        ReferenceInfo info = newInfo(column, type, columnPolicy, indexType);
        // don't add it if there is a partitioned equivalent of this column
        if (partitioned || !(partitionedBy != null && partitionedBy.contains(column))) {
            if (info.ident().isColumn()) {
                columnsBuilder.add(info);
            }
            referencesBuilder.put(info.ident().columnIdent(), info);
        }
        if (partitioned) {
            partitionedByColumnsBuilder.add(info);
        }
    }

    private ReferenceInfo newInfo(ColumnIdent column,
                                  DataType type,
                                  ColumnPolicy columnPolicy,
                                  ReferenceInfo.IndexType indexType) {
        RowGranularity granularity = RowGranularity.DOC;
        if (partitionedBy.contains(column)) {
            granularity = RowGranularity.PARTITION;
        }
        return new ReferenceInfo(new ReferenceIdent(ident, column), granularity, type,
                columnPolicy, indexType);
    }

    /**
     * extract dataType from given columnProperties
     *
     * @param columnProperties map of String to Object containing column properties
     * @return dataType of the column with columnProperties
     */
    public static DataType getColumnDataType(Map<String, Object> columnProperties) {
        DataType type;
        String typeName = (String) columnProperties.get("type");

        if (typeName == null) {
            if (columnProperties.containsKey("properties")) {
                type = DataTypes.OBJECT;
            } else {
                return DataTypes.NOT_SUPPORTED;
            }
        } else if (typeName.equalsIgnoreCase("array")) {

            Map<String, Object> innerProperties = getNested(columnProperties, "inner");
            DataType innerType = getColumnDataType(innerProperties);
            type = new ArrayType(innerType);
        } else {
            typeName = typeName.toLowerCase(Locale.ENGLISH);
            type = MoreObjects.firstNonNull(dataTypeMap.get(typeName), DataTypes.NOT_SUPPORTED);
        }
        return type;
    }

    private ReferenceInfo.IndexType getColumnIndexType(Map<String, Object> columnProperties) {
        String indexType = (String) columnProperties.get("index");
        String analyzerName = (String) columnProperties.get("analyzer");
        if (indexType != null) {
            if (indexType.equals(ReferenceInfo.IndexType.NOT_ANALYZED.toString())) {
                return ReferenceInfo.IndexType.NOT_ANALYZED;
            } else if (indexType.equals(ReferenceInfo.IndexType.NO.toString())) {
                return ReferenceInfo.IndexType.NO;
            } else if (indexType.equals(ReferenceInfo.IndexType.ANALYZED.toString())
                    && analyzerName != null && !analyzerName.equals("keyword")) {
                return ReferenceInfo.IndexType.ANALYZED;
            }
        } // default indexType is analyzed so need to check analyzerName if indexType is null
        else if (analyzerName != null && !analyzerName.equals("keyword")) {
            return ReferenceInfo.IndexType.ANALYZED;
        }
        return ReferenceInfo.IndexType.NOT_ANALYZED;
    }

    private ColumnIdent childIdent(ColumnIdent ident, String name) {
        if (ident == null) {
            return new ColumnIdent(name);
        }
        if (ident.isColumn()) {
            return new ColumnIdent(ident.name(), name);
        } else {
            ImmutableList.Builder<String> builder = ImmutableList.builder();
            for (String s : ident.path()) {
                builder.add(s);
            }
            builder.add(name);
            return new ColumnIdent(ident.name(), builder.build());
        }
    }

    /**
     * extracts index definitions as well
     */
    @SuppressWarnings("unchecked")
    private void internalExtractColumnDefinitions(ColumnIdent columnIdent,
                                                  Map<String, Object> propertiesMap) {
        if (propertiesMap == null) {
            return;
        }

        for (Map.Entry<String, Object> columnEntry : propertiesMap.entrySet()) {
            Map<String, Object> columnProperties = (Map) columnEntry.getValue();
            DataType columnDataType = getColumnDataType(columnProperties);
            ColumnIdent newIdent = childIdent(columnIdent, columnEntry.getKey());

            columnProperties = furtherColumnProperties(columnProperties);
            ReferenceInfo.IndexType columnIndexType = getColumnIndexType(columnProperties);
            if (columnDataType == DataTypes.OBJECT
                    || (columnDataType.id() == ArrayType.ID
                    && ((ArrayType) columnDataType).innerType() == DataTypes.OBJECT)) {
                ColumnPolicy columnPolicy =
                        ColumnPolicy.of(columnProperties.get("dynamic"));
                add(newIdent, columnDataType, columnPolicy, ReferenceInfo.IndexType.NO, false);

                if (columnProperties.get("properties") != null) {
                    // walk nested
                    internalExtractColumnDefinitions(newIdent, (Map<String, Object>) columnProperties.get("properties"));
                }
            } else if (columnDataType != DataTypes.NOT_SUPPORTED) {
                List<String> copyToColumns = getNested(columnProperties, "copy_to");

                // extract columns this column is copied to, needed for indices
                if (copyToColumns != null) {
                    for (String copyToColumn : copyToColumns) {
                        ColumnIdent targetIdent = ColumnIdent.fromPath(copyToColumn);
                        IndexReferenceInfo.Builder builder = getOrCreateIndexBuilder(targetIdent);
                        builder.addColumn(newInfo(newIdent, columnDataType, ColumnPolicy.DYNAMIC, columnIndexType));
                    }
                }
                // is it an index?
                if (indicesMap.containsKey(newIdent.fqn())) {
                    IndexReferenceInfo.Builder builder = getOrCreateIndexBuilder(newIdent);
                    builder.indexType(columnIndexType)
                            .ident(new ReferenceIdent(ident, newIdent));
                } else {
                    add(newIdent, columnDataType, columnIndexType);
                }
            }
        }
    }

    /**
     * get the real column properties from a possible array mapping,
     * keeping most of this stuff inside "inner"
     */
    private Map<String, Object> furtherColumnProperties(Map<String, Object> columnProperties) {
        if (columnProperties.get("inner") != null) {
            return (Map<String, Object>) columnProperties.get("inner");
        } else {
            return columnProperties;
        }
    }

    private IndexReferenceInfo.Builder getOrCreateIndexBuilder(ColumnIdent ident) {
        IndexReferenceInfo.Builder builder = indicesBuilder.get(ident);
        if (builder == null) {
            builder = new IndexReferenceInfo.Builder();
            indicesBuilder.put(ident, builder);
        }
        return builder;
    }

    private ImmutableList<ColumnIdent> getPrimaryKey() {
        Map<String, Object> metaMap = getNested(defaultMappingMap, "_meta");
        if (metaMap == null) {
            hasAutoGeneratedPrimaryKey = true;
            return ImmutableList.of(ID_IDENT);
        }

        ImmutableList.Builder<ColumnIdent> builder = ImmutableList.builder();
        Object pKeys = metaMap.get("primary_keys");
        if (pKeys == null) {
            hasAutoGeneratedPrimaryKey = true;
            return ImmutableList.of(ID_IDENT);
        }

        if (pKeys instanceof String) {
            builder.add(ColumnIdent.fromPath((String) pKeys));
        } else if (pKeys instanceof Collection) {
            Collection keys = (Collection) pKeys;
            if (keys.isEmpty()) {
                hasAutoGeneratedPrimaryKey = true;
                return ImmutableList.of(ID_IDENT);
            }
            for (Object pkey : keys) {
                builder.add(ColumnIdent.fromPath(pkey.toString()));
            }
        }
        return builder.build();
    }

    private ImmutableList<ColumnIdent> getPartitionedBy() {
        ImmutableList.Builder<ColumnIdent> builder = ImmutableList.builder();
        for (List<String> partitionedByInfo : partitionedByList) {
            builder.add(ColumnIdent.fromPath(partitionedByInfo.get(0)));
        }
        return builder.build();
    }

    private ColumnPolicy getColumnPolicy() {
        Object dynamic = getNested(defaultMappingMap, "dynamic");
        if (ColumnPolicy.STRICT.value().equals(String.valueOf(dynamic).toLowerCase(Locale.ENGLISH))) {
            return ColumnPolicy.STRICT;
        } else if (Booleans.isExplicitFalse(String.valueOf(dynamic))) {
            return ColumnPolicy.IGNORED;
        } else {
            return ColumnPolicy.DYNAMIC;
        }
    }

    private void createColumnDefinitions() {
        Map<String, Object> propertiesMap = getNested(defaultMappingMap, "properties");
        internalExtractColumnDefinitions(null, propertiesMap);
        extractPartitionedByColumns();
    }

    private ImmutableMap<ColumnIdent, IndexReferenceInfo> createIndexDefinitions() {
        ImmutableMap.Builder<ColumnIdent, IndexReferenceInfo> builder = ImmutableMap.builder();
        for (Map.Entry<ColumnIdent, IndexReferenceInfo.Builder> entry : indicesBuilder.entrySet()) {
            builder.put(entry.getKey(), entry.getValue().build());
        }
        indices = builder.build();
        return indices;
    }

    private void extractPartitionedByColumns() {
        for (Tuple<ColumnIdent, DataType> partitioned : PartitionedByMappingExtractor.extractPartitionedByColumns(partitionedByList)) {
            addPartitioned(partitioned.v1(), partitioned.v2());
        }
    }

    private ColumnIdent getRoutingCol() {
        if (defaultMappingMetaData != null) {
            Map<String, Object> metaMap = getNested(defaultMappingMap, "_meta");
            if (metaMap != null) {
                String routingPath = (String) metaMap.get("routing");
                if (routingPath != null) {
                    return ColumnIdent.fromPath(routingPath);
                }
            }
        }
        if (primaryKey.size() == 1) {
            return primaryKey.get(0);
        }
        return ID_IDENT;
    }

    public DocIndexMetaData build() {
        partitionedBy = getPartitionedBy();
        columnPolicy = getColumnPolicy();
        createColumnDefinitions();
        indices = createIndexDefinitions();
        columns = ImmutableList.copyOf(columnsBuilder.build());
        partitionedByColumns = partitionedByColumnsBuilder.build();

        for (Tuple<ColumnIdent, ReferenceInfo> sysColumn : DocSysColumns.forTable(ident)) {
            referencesBuilder.put(sysColumn.v1(), sysColumn.v2());
        }
        references = referencesBuilder.build();
        primaryKey = getPrimaryKey();
        routingCol = getRoutingCol();
        return this;
    }

    public ImmutableMap<ColumnIdent, ReferenceInfo> references() {
        return references;
    }

    public ImmutableList<ReferenceInfo> columns() {
        return columns;
    }

    public ImmutableMap<ColumnIdent, IndexReferenceInfo> indices() {
        return indices;
    }

    public ImmutableList<ReferenceInfo> partitionedByColumns() {
        return partitionedByColumns;
    }

    public ImmutableList<ColumnIdent> primaryKey() {
        return primaryKey;
    }

    public ColumnIdent routingCol() {
        return routingCol;
    }

    /**
     * Returns true if the schema of this and <code>other</code> is the same,
     * this includes the table name, as this is reflected in the ReferenceIdents of
     * the columns.
     */
    public boolean schemaEquals(DocIndexMetaData other) {
        if (this == other) return true;
        if (other == null) return false;

        // TODO: when analyzers are exposed in the info, equality has to be checked on them
        // see: TransportSQLActionTest.testSelectTableAliasSchemaExceptionColumnDefinition
        if (columns != null ? !columns.equals(other.columns) : other.columns != null) return false;
        if (primaryKey != null ? !primaryKey.equals(other.primaryKey) : other.primaryKey != null) return false;
        if (indices != null ? !indices.equals(other.indices) : other.indices != null) return false;
        if (references != null ? !references.equals(other.references) : other.references != null) return false;
        if (routingCol != null ? !routingCol.equals(other.routingCol) : other.routingCol != null) return false;

        return true;
    }

    protected DocIndexMetaData merge(DocIndexMetaData other,
                                     TransportPutIndexTemplateAction transportPutIndexTemplateAction,
                                     boolean thisIsCreatedFromTemplate) throws IOException {
        if (schemaEquals(other)) {
            return this;
        } else if (thisIsCreatedFromTemplate) {
            if (this.references.size() < other.references.size()) {
                // this is older, update template and return other
                // settings in template are always authoritative for table information about
                // number_of_shards and number_of_replicas
                updateTemplate(other, transportPutIndexTemplateAction, this.metaData.settings());
                // merge the new mapping with the template settings
                return new DocIndexMetaData(IndexMetaData.builder(other.metaData).settings(this.metaData.settings()).build(), other.ident).build();
            } else if (references().size() == other.references().size() &&
                    !references().keySet().equals(other.references().keySet())) {
                XContentHelper.update(defaultMappingMap, other.defaultMappingMap, false);
                // update the template with new information
                updateTemplate(this, transportPutIndexTemplateAction, this.metaData.settings());
                return this;
            }
            // other is older, just return this
            return this;
        } else {
            throw new TableAliasSchemaException(other.ident.name());
        }
    }

    private void updateTemplate(DocIndexMetaData md,
                                TransportPutIndexTemplateAction transportPutIndexTemplateAction,
                                Settings updateSettings) {
        String templateName = PartitionName.templateName(ident.schema(), ident.name());
        PutIndexTemplateRequest request = new PutIndexTemplateRequest(templateName)
                .mapping(Constants.DEFAULT_MAPPING_TYPE, md.defaultMappingMap)
                .create(false)
                .settings(updateSettings)
                .template(templateName + "*");
        for (String alias : md.aliases()) {
            request = request.alias(new Alias(alias));
        }
        transportPutIndexTemplateAction.execute(request);
    }

    /**
     * @return the name of the underlying index even if this table is referenced by alias
     */
    public String concreteIndexName() {
        return metaData.index();
    }

    public boolean isAlias() {
        return isAlias;
    }

    public Set<String> aliases() {
        return aliases;
    }

    public boolean hasAutoGeneratedPrimaryKey() {
        return hasAutoGeneratedPrimaryKey;
    }

    public int numberOfShards() {
        return numberOfShards;
    }

    public BytesRef numberOfReplicas() {
        return numberOfReplicas;
    }

    public ImmutableList<ColumnIdent> partitionedBy() {
        return partitionedBy;
    }

    public ColumnPolicy columnPolicy() {
        return columnPolicy;
    }

    public ImmutableMap<String, Object> tableParameters() {
        return tableParameters;
    }
}
