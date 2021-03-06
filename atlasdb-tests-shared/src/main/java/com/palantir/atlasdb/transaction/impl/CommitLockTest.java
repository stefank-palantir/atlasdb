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

package com.palantir.atlasdb.transaction.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Optional;

import org.junit.Assume;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.impl.logging.CommitProfileProcessor;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.AtlasCellLockDescriptor;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;

@RunWith(Theories.class)
public class CommitLockTest extends TransactionTestSetup {
    private static final String ROW = "row";
    private static final String COLUMN = "col_1";
    private static final String OTHER_COLUMN = "col_2";

    @DataPoints
    public static ConflictHandler[] conflictHandlers = ConflictHandler.values();

    @Override
    protected KeyValueService getKeyValueService() {
        return new InMemoryKeyValueService(false,
                PTExecutors.newSingleThreadExecutor(PTExecutors.newNamedThreadFactory(false)));
    }

    @Theory
    public void shouldAcquireRowLockIfLocksAtRowLevel(ConflictHandler conflictHandler) {
        Assume.assumeTrue(conflictHandler.lockRowsForConflicts());

        PreCommitCondition rowLocksAcquired = (ignored) -> {
            LockResponse response = acquireRowLock(ROW);
            assertFalse(response.wasSuccessful());
        };

        commitWriteWith(rowLocksAcquired, conflictHandler);
    }

    @Theory
    public void shouldAcquireCellLockIfLocksAtCellLevel(ConflictHandler conflictHandler) {
        Assume.assumeTrue(conflictHandler.lockCellsForConflicts());

        PreCommitCondition cellLocksAcquired = (ignored) -> {
            LockResponse response = acquireCellLock(ROW, COLUMN);
            assertFalse(response.wasSuccessful());
        };

        commitWriteWith(cellLocksAcquired, conflictHandler);
    }

    @Theory
    public void shouldNotAcquireRowLockIfDoesNotLockAtRowLevel(ConflictHandler conflictHandler) {
        Assume.assumeFalse(conflictHandler.lockRowsForConflicts());

        PreCommitCondition canAcquireRowLock = (ignored) -> {
            LockResponse response = acquireRowLock(ROW);
            assertTrue(response.wasSuccessful());
        };

        commitWriteWith(canAcquireRowLock, conflictHandler);
    }

    @Theory
    public void shouldNotAcquireCellLockIfDoesNotLockAtCellLevel(ConflictHandler conflictHandler) {
        Assume.assumeFalse(conflictHandler.lockCellsForConflicts());

        PreCommitCondition canAcquireCellLock = (ignored) -> {
            LockResponse response = acquireCellLock(ROW, COLUMN);
            //current lock implementation allows you to get a cell lock on a row that is already locked
            assertTrue(response.wasSuccessful());
        };

        commitWriteWith(canAcquireCellLock, conflictHandler);
    }

    @Theory
    public void shouldAcquireRowAndCellLockIfRequiresBoth(ConflictHandler conflictHandler) {
        Assume.assumeTrue(conflictHandler.lockCellsForConflicts() && conflictHandler.lockRowsForConflicts());

        PreCommitCondition cellAndRowLockAcquired = (ignored) -> {
            LockResponse cellLockResponse = acquireCellLock(ROW, COLUMN);
            LockResponse rowLockResponse = acquireRowLock(ROW);

            assertFalse(cellLockResponse.wasSuccessful());
            assertFalse(rowLockResponse.wasSuccessful());
        };

        commitWriteWith(cellAndRowLockAcquired, conflictHandler);
    }

    @Theory
    public void canAcquireLockOnMultipleCellsOnSameRow(ConflictHandler conflictHandler) {
        Assume.assumeTrue(conflictHandler.lockCellsForConflicts());

        PreCommitCondition canAcquireLockOnDifferentCell = (ignored) -> {
            LockResponse response = acquireCellLock(ROW, OTHER_COLUMN);
            assertTrue(response.wasSuccessful());
        };

        commitWriteWith(canAcquireLockOnDifferentCell, conflictHandler);
    }

    private void commitWriteWith(PreCommitCondition preCommitCondition, ConflictHandler conflictHandler) {
        Transaction transaction = startTransaction(preCommitCondition, conflictHandler);
        put(transaction, ROW, COLUMN, "100");
        transaction.commit();
    }

    private Transaction startTransaction(PreCommitCondition preCommitCondition, ConflictHandler conflictHandler) {
        ImmutableMap<TableReference, ConflictHandler> tablesToWriteWrite = ImmutableMap.of(
                TEST_TABLE,
                conflictHandler,
                TransactionConstants.TRANSACTION_TABLE,
                ConflictHandler.IGNORE_ALL);
        return new SerializableTransaction(
                MetricsManagers.createForTests(),
                keyValueService,
                timelockService,
                transactionService,
                NoOpCleaner.INSTANCE,
                Suppliers.ofInstance(timestampService.getFreshTimestamp()),
                TestConflictDetectionManagers.createWithStaticConflictDetection(tablesToWriteWrite),
                SweepStrategyManagers.createDefault(keyValueService),
                0L,
                Optional.empty(),
                preCommitCondition,
                AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING,
                null,
                TransactionReadSentinelBehavior.THROW_EXCEPTION,
                true,
                timestampCache,
                AtlasDbConstants.DEFAULT_TRANSACTION_LOCK_ACQUIRE_TIMEOUT_MS,
                AbstractTransactionTest.GET_RANGES_EXECUTOR,
                AbstractTransactionTest.DEFAULT_GET_RANGES_CONCURRENCY,
                MultiTableSweepQueueWriter.NO_OP,
                MoreExecutors.newDirectExecutorService(),
                CommitProfileProcessor.createNonLogging(metricsManager),
                true) {
            @Override
            protected Map<Cell, byte[]> transformGetsForTesting(Map<Cell, byte[]> map) {
                return Maps.transformValues(map, input -> input.clone());
            }
        };
    }

    private LockResponse acquireRowLock(String rowName) {
        LockDescriptor rowLockDescriptor = AtlasRowLockDescriptor.of(
                TEST_TABLE.getQualifiedName(),
                PtBytes.toBytes(rowName));
        return lock(rowLockDescriptor);
    }

    private LockResponse acquireCellLock(String rowName, String colName) {
        LockDescriptor cellLockDescriptor = AtlasCellLockDescriptor.of(
                TEST_TABLE.getQualifiedName(),
                PtBytes.toBytes(rowName),
                PtBytes.toBytes(colName));
        return lock(cellLockDescriptor);
    }

    private LockResponse lock(LockDescriptor lockDescriptor) {
        LockRequest lockRequest = LockRequest.of(ImmutableSet.of(lockDescriptor), 5_000);
        return timelockService.lock(lockRequest);
    }
}
