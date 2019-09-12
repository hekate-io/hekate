/*
 * Copyright 2019 The Hekate Project
 *
 * The Hekate Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.hekate.lock.internal;

import io.hekate.lock.internal.LockProtocol.MigrationApplyRequest;
import io.hekate.lock.internal.LockProtocol.MigrationPrepareRequest;
import io.hekate.partition.PartitionMapper;

interface LockMigrationSpy {
    default void onAfterPrepareSent(MigrationPrepareRequest request) {
        // No-op.
    }

    default void onPrepareReceived(MigrationPrepareRequest request) {
        // No-op.
    }

    default void onAfterApplySent(MigrationApplyRequest request) {
        // No-op.
    }

    default void onApplyReceived(MigrationApplyRequest request) {
        // No-op.
    }

    default void onTopologyChange(PartitionMapper mapper) {
        // No-op.
    }
}
