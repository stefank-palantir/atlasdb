/**
 * Copyright 2016 Palantir Technologies
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

import java.util.Iterator;
import java.util.List;

import org.immutables.value.Value;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;

@Value.Immutable
public abstract class CellsAndTimestamps {

    public abstract List<CellAndTimestamps> cellAndTimestampsList();

    public static CellsAndTimestamps of(List<CellAndTimestamps> cellAndTimestampsList) {
        return ImmutableCellsAndTimestamps.builder().cellAndTimestampsList(cellAndTimestampsList).build();
    }

    public static CellsAndTimestamps fromIterator(Iterator<CellAndTimestamps> iterator) {
        return of(ImmutableList.copyOf(iterator));
    }

    public Multimap<Cell, Long> asMultimap() {
        ImmutableMultimap.Builder<Cell, Long> cellTsMappings = ImmutableMultimap.builder();
        for (CellAndTimestamps cellAndTimestamps : cellAndTimestampsList()) {
            cellTsMappings.putAll(cellAndTimestamps.cell(), cellAndTimestamps.timestamps());
        }
        return cellTsMappings.build();
    }

    public boolean isEmpty() {
        return cellAndTimestampsList().isEmpty();
    }

    public int size() {
        return cellAndTimestampsList().size();
    }
}
