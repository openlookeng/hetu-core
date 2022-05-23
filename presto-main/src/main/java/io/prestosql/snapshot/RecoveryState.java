/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.snapshot;

public enum RecoveryState {
    DEFAULT, // Bootstrapped with necessary callbacks and configs, ready to use
    STOPPING_FOR_RESCHEDULE, // Stopping the query for rescheduling after receiving the failure is in progress
    SUSPENDED, // Suspend execution for the Query
    RESCHEDULING, // Restarting / Resuming the query based on recovery implementation, Move back to READY state after recovery completed
    RECOVERY_FAILED
}
