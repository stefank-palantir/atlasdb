/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.api;

import org.immutables.value.Value;

import com.palantir.lock.v2.LockToken;

@Value.Immutable
public interface TransactionAndImmutableTsLock {

    Transaction transaction();

    LockToken immutableTsLock();

    static TransactionAndImmutableTsLock of(Transaction transaction, LockToken immutableTsLock) {
        return ImmutableTransactionAndImmutableTsLock.builder()
                .transaction(transaction)
                .immutableTsLock(immutableTsLock)
                .build();
    }
}
