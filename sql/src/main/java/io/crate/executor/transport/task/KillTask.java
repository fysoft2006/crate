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

package io.crate.executor.transport.task;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.executor.JobTask;
import io.crate.executor.TaskResult;

import java.util.List;
import java.util.UUID;

public class KillTask extends JobTask {

    // TODO: add kill transport
    public KillTask(UUID jobId) {
        super(jobId);
    }

    @Override
    public void start() {
        // TODO: do something
    }

    @Override
    public List<ListenableFuture<TaskResult>> result() {
        return ImmutableList.of(
                Futures.<TaskResult>immediateFailedFuture(
                        new UnsupportedOperationException("KILL statement not supported")
                )
        );
    }

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
        // ignore
    }
}