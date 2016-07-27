/**
 * Copyright 2016 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.sweep;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Consumer;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;

public class ConservativeSweepStrategySweeper implements SweepStrategySweeper {
    private final KeyValueService keyValueService;
    private final Supplier<Long> immutableTimestampSupplier;
    private final Supplier<Long> unreadableTimestampSupplier;

    public ConservativeSweepStrategySweeper(KeyValueService keyValueService, Supplier<Long> immutableTimestampSupplier, Supplier<Long> unreadableTimestampSupplier) {
        this.keyValueService = keyValueService;
        this.immutableTimestampSupplier = immutableTimestampSupplier;
        this.unreadableTimestampSupplier = unreadableTimestampSupplier;
    }

    @Override
    public long getSweepTimestamp() {
        return Math.min(unreadableTimestampSupplier.get(), immutableTimestampSupplier.get());
    }

    @Override
    public ClosableIterator<RowResult<Value>> getValues(TableReference tableReference, RangeRequest rangeRequest, long timestamp) {
        return ClosableIterators.emptyImmutableClosableIterator();
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getCellTimestamps(TableReference tableReference, RangeRequest rangeRequest, long timestamp) {
        ClosableIterator<RowResult<Value>> range = keyValueService.getRange(tableReference, rangeRequest, timestamp);

        Set<byte[]> rowNames = new HashSet<>();
        for (RowResult<Value> rowResult : range.items()) {
            rowNames.add(rowResult.getRowName());
        }

        // TODO null?
        ColumnRangeSelection columnRangeSelection = new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1);

        // let's pretend the key here is a row
        // TODO we want something more like keyValueService.getRowsColumnRangeHistory(...)
        Map<byte[], RowColumnRangeIterator> rowsColumnRange = keyValueService.getRowsColumnRange(tableReference, rowNames, columnRangeSelection, timestamp);


        Set<RowResult<Set<Long>>> rowResults = new HashSet<>();

        for (byte[] row : rowsColumnRange.keySet()) {
            RowColumnRangeIterator rowColumnRangeIterator = rowsColumnRange.get(row);

            SortedMap<byte[], Set<Long>> columnToTimestamp = new TreeMap<>();

            // TODO is this the right way to use this iterator???
            rowColumnRangeIterator.forEachRemaining(new Consumer<Map.Entry<Cell, Value>>() {
                @Override
                public void accept(Map.Entry<Cell, Value> cellValueEntry) {
                    long currentTimestamp = cellValueEntry.getValue().getTimestamp();
                    Cell currentCell = cellValueEntry.getKey();

                    // TODO This needs to be a set, so we get all historical timestamps!
                    columnToTimestamp.put(currentCell.getColumnName(), ImmutableSet.of(currentTimestamp));

                }
            });


            rowResults.add(RowResult.create(row, columnToTimestamp));
        }


        return ClosableIterators.wrap(rowResults.iterator());


//        return keyValueService.getRangeOfTimestamps(tableReference, rangeRequest, timestamp);
    }

    @Override
    public Set<Long> getTimestampsToIgnore() {
        return ImmutableSet.of(Value.INVALID_VALUE_TIMESTAMP);
    }

    @Override
    public boolean shouldAddSentinels() {
        return true;
    }
}
