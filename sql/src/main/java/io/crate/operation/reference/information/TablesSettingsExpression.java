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

package io.crate.operation.reference.information;

import io.crate.analyze.TableParameterInfo;
import io.crate.metadata.information.InformationTablesTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.reference.RowCollectNestedObjectExpression;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

public class TablesSettingsExpression extends RowCollectNestedObjectExpression<TableInfo> {

    public final static String NAME = "settings";

    public TablesSettingsExpression() {
        super(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS);
        addChildImplementations();
    }

    private void addChildImplementations() {
        childImplementations.put(TablesSettingsBlocksExpression.NAME, new TablesSettingsBlocksExpression());
        childImplementations.put(TablesSettingsRoutingExpression.NAME, new TablesSettingsRoutingExpression());
        childImplementations.put(TablesSettingsRecoveryInitialShards.NAME, new TablesSettingsRecoveryInitialShards());
        childImplementations.put(TablesSettingsWarmerEnabled.NAME, new TablesSettingsWarmerEnabled());
        childImplementations.put(TablesSettingsTranslogExpression.NAME, new TablesSettingsTranslogExpression());
        // todo all other settings
    }

    static class TablesSettingsBlocksExpression extends RowCollectNestedObjectExpression<TableInfo> {

        public final static String NAME = "blocks";

        public TablesSettingsBlocksExpression() {
            super(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_BLOCKS);
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put("read_only",
                    new InformationTablesExpression<Boolean>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_BLOCKS_READ_ONLY) {
                        @Override
                        public Boolean value() {
                            return (Boolean) this.row.tableParameters().get(TableParameterInfo.READ_ONLY);
                        }
                    });
            childImplementations.put("read",
                    new InformationTablesExpression<Boolean>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_BLOCKS_READ) {
                        @Override
                        public Boolean value() {
                            return (Boolean) this.row.tableParameters().get(TableParameterInfo.BLOCKS_READ);
                        }
                    });
            childImplementations.put("write",
                    new InformationTablesExpression<Boolean>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_BLOCKS_WRITE) {
                        @Override
                        public Boolean value() {
                            return (Boolean) this.row.tableParameters().get(TableParameterInfo.BLOCKS_WRITE);
                        }
                    });
            childImplementations.put("metadata",
                    new InformationTablesExpression<Boolean>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_BLOCKS_METADATA) {
                        @Override
                        public Boolean value() {
                            return (Boolean) this.row.tableParameters().get(TableParameterInfo.BLOCKS_METADATA);
                        }
                    });
        }
    }

    static class TablesSettingsRoutingExpression extends RowCollectNestedObjectExpression<TableInfo> {

        public final static String NAME = "routing.allocation";

        public TablesSettingsRoutingExpression() {
            super(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_ROUTING_ALLOCATION);
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put("enable",
                    new InformationTablesExpression<String>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_ROUTING_ALLOCATION_ENABLE) {
                        @Override
                        public String value() {
                            return (String) this.row.tableParameters().get(TableParameterInfo.ROUTING_ALLOCATION_ENABLE);
                        }
                    });
            childImplementations.put("total_shards_per_node",
                    new InformationTablesExpression<Integer>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_ROUTING_ALLOCATION_TOTAL_SHARDS_PER_NODE) {
                        @Override
                        public Integer value() {
                            return (Integer) this.row.tableParameters().get(TableParameterInfo.TOTAL_SHARDS_PER_NODE);
                        }
                    });
        }
    }

    static class TablesSettingsRecoveryInitialShards extends RowCollectNestedObjectExpression<TableInfo> {

        public final static String NAME = "recovery";

        public TablesSettingsRecoveryInitialShards() {
            super(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_RECOVERY);
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put("initial_shards",
                    new InformationTablesExpression<String>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_RECOVERY_INITIAL_SHARDS) {
                        @Override
                        public String value() {
                            return (String) this.row.tableParameters().get(TableParameterInfo.RECOVERY_INITIAL_SHARDS);
                        }
                    });
        }
    }

    static class TablesSettingsWarmerEnabled extends RowCollectNestedObjectExpression<TableInfo> {

        public final static String NAME = "warmer";

        public TablesSettingsWarmerEnabled() {
            super(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_WARMER);
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put("enabled",
                    new InformationTablesExpression<Boolean>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_WARMER_ENABLED) {
                        @Override
                        public Boolean value() {
                            return (Boolean) this.row.tableParameters().get(TableParameterInfo.WARMER_ENABLED);
                        }
                    });
        }
    }

    static class TablesSettingsTranslogExpression extends RowCollectNestedObjectExpression<TableInfo> {

        public final static String NAME = "translog";

        public TablesSettingsTranslogExpression() {
            super(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_TRANSLOG);
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put("flush_threshold_ops",
                    new InformationTablesExpression<Integer>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_OPS) {
                        @Override
                        public Integer value() {
                            return (Integer) this.row.tableParameters().get(TableParameterInfo.FLUSH_THRESHOLD_OPS);
                        }
                    });
            childImplementations.put("flush_threshold_size",
                    new InformationTablesExpression<String>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE) {
                        @Override
                        public String value() {
                            ByteSizeValue value = (ByteSizeValue) this.row.tableParameters().get(TableParameterInfo.FLUSH_THRESHOLD_SIZE);
                            return value != null ? value.toString() : null;
                        }
                    });
            childImplementations.put("flush_threshold_period",
                    new InformationTablesExpression<String>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_PERIOD) {
                        @Override
                        public String value() {
                            TimeValue value = (TimeValue) this.row.tableParameters().get(TableParameterInfo.FLUSH_THRESHOLD_PERIOD);
                            return value != null ? value.toString() : null;
                        }
                    });
            childImplementations.put("disable_flush",
                    new InformationTablesExpression<Boolean>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_TRANSLOG_DISABLE_FLUSH) {
                        @Override
                        public Boolean value() {
                            return (Boolean) this.row.tableParameters().get(TableParameterInfo.FLUSH_DISABLE);
                        }
                    });
            childImplementations.put("interval",
                    new InformationTablesExpression<String>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_TRANSLOG_INTERVAL) {
                        @Override
                        public String value() {
                            TimeValue value = (TimeValue) this.row.tableParameters().get(TableParameterInfo.TRANSLOG_INTERVAL);
                            return value != null ? value.toString() : null;
                        }
                    });
        }
    }
}
