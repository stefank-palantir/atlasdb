/**
 * Copyright 2017 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.sweep;

import org.mpierce.metrics.reservoir.hdrhistogram.HdrHistogramReservoir;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.util.AtlasDbMetrics;

class SweepMetrics {
    private static final String STALE_VALUES_DELETED = "staleValuesDeleted";
    private static final String CELLS_EXAMINED = "cellsExamined";
    private static final String AGGREGATE_CELLS_DELETED = MetricRegistry.name(SweepMetrics.class,
            STALE_VALUES_DELETED);
    private static final String AGGREGATE_CELLS_EXAMINED = MetricRegistry.name(SweepMetrics.class,
            CELLS_EXAMINED);

    private final MetricRegistry metricRegistry;

    public static SweepMetrics create() {
        SweepMetrics sweepMetrics = new SweepMetrics(AtlasDbMetrics.getMetricRegistry());
        sweepMetrics.registerAggregateMetrics();
        return sweepMetrics;
    }

    protected SweepMetrics(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    private void registerAggregateMetrics() {
        registerMetricWithHdrHistogram(AGGREGATE_CELLS_DELETED);
        registerMetricWithHdrHistogram(AGGREGATE_CELLS_EXAMINED);
    }

    void registerMetricsIfNecessary(TableReference tableRef) {
        registerMetricWithHdrHistogram(getCellsDeletedMetric(tableRef));
        registerMetricWithHdrHistogram(getCellsExaminedMetric(tableRef));
    }

    void recordMetrics(TableReference tableRef, SweepResults results) {
        recordCellsDeleted(tableRef, results.getCellsDeleted());
        recordCellsExamined(tableRef, results.getCellsExamined());
    }

    private void recordCellsDeleted(TableReference tableRef, long cellsDeleted) {
        String deletesMetric = getCellsDeletedMetric(tableRef);

        metricRegistry.histogram(deletesMetric).update(cellsDeleted);
        metricRegistry.histogram(AGGREGATE_CELLS_DELETED).update(cellsDeleted);
    }

    private void recordCellsExamined(TableReference tableRef, long cellsExamined) {
        String examinedMetric = getCellsExaminedMetric(tableRef);

        metricRegistry.histogram(examinedMetric).update(cellsExamined);
        metricRegistry.histogram(AGGREGATE_CELLS_EXAMINED).update(cellsExamined);
    }

    private String getCellsExaminedMetric(TableReference tableRef) {
        return MetricRegistry.name(SweepMetrics.class, CELLS_EXAMINED, tableRef.getQualifiedName());
    }

    private String getCellsDeletedMetric(TableReference tableRef) {
        return MetricRegistry.name(SweepMetrics.class, STALE_VALUES_DELETED, tableRef.getQualifiedName());
    }

    private void registerMetricWithHdrHistogram(String deletesMetric) {
        registerMetricIfNotExists(deletesMetric, new Histogram(new HdrHistogramReservoir()));
    }

    private void registerMetricIfNotExists(String name, Metric metric) {
        if (!metricRegistry.getMetrics().containsKey(name)) {
            metricRegistry.register(name, metric);
        }
    }
}
