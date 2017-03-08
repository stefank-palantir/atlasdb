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

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.util.AtlasDbMetrics;

final class SweepMetrics {
    private final MetricRegistry metricRegistry;

    public static SweepMetrics create() {
        SweepMetrics sweepMetrics = new SweepMetrics();
        sweepMetrics.registerAggregateMetrics();
        return sweepMetrics;
    }

    private void registerAggregateMetrics() {
        SlidingTimeWindowReservoir reservoir = new SlidingTimeWindowReservoir(1, TimeUnit.DAYS);
        Histogram slidingWeek = new Histogram(reservoir);
        String deletes = MetricRegistry.name(SweepMetrics.class, "totalDeletes");

        registerMetricIfNotExists(deletes, slidingWeek);
    }

    private SweepMetrics() {
        this.metricRegistry = AtlasDbMetrics.getMetricRegistry();
    }

    void registerMetricsIfNecessary(TableReference tableRef) {
        SlidingTimeWindowReservoir reservoir = new SlidingTimeWindowReservoir(7, TimeUnit.DAYS);
        Histogram slidingWeek = new Histogram(reservoir);
        String deletesMetric = MetricRegistry.name(SweepMetrics.class, "deletes", tableRef.getQualifiedName());

        registerMetricIfNotExists(deletesMetric, slidingWeek);
    }

    private void registerMetricIfNotExists(String name, Metric metric) {
        if (!metricRegistry.getMetrics().containsKey(name)) {
            metricRegistry.register(name, metric);
        }
    }

    // TODO this will obviously change when we have many metrics.
    // Will probably end up passing in a SweepProgressRowResult object.
    void recordMetrics(TableReference tableRef, long cellsDeleted) {
        String deletesMetric = MetricRegistry.name(SweepMetrics.class, "deletes", tableRef.getQualifiedName());
        metricRegistry.histogram(deletesMetric).update(cellsDeleted);

        String totalDeletes = MetricRegistry.name(SweepMetrics.class, "totalDeletes");
        metricRegistry.histogram(totalDeletes).update(cellsDeleted);
    }

}
