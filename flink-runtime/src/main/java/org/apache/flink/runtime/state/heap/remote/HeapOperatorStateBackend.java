/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap.remote;

import java.util.List;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.SnapshotResult;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.RunnableFuture;

import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.remote.RemoteHeapKeyedStateBackend.RemoteHeapKvStateInfo;
import org.apache.flink.util.FlinkRuntimeException;

/**
 * Default implementation of OperatorStateStore that provides the ability to make snapshots.
 */
@Internal
public class HeapOperatorStateBackend implements OperatorStateBackend {

	LinkedHashMap<String, RemoteHeapKvStateInfo> kvStateInformation;
	RemoteKVSyncClient syncRemClient;
	RemoteKVAsyncClient asyncRemClient;

	public HeapOperatorStateBackend(
		LinkedHashMap<String, RemoteHeapKvStateInfo> kvStateInformation,
		RemoteKVSyncClient syncRemClient, RemoteKVAsyncClient asyncRemClient) {
		this.kvStateInformation = kvStateInformation;
		this.syncRemClient = syncRemClient;
		this.asyncRemClient = asyncRemClient;

	}

	@Override
	public void dispose() {

	}

	@Override
	public void close() throws IOException {

	}

	@Override
	public <K, V> BroadcastState<K, V> getBroadcastState(
		MapStateDescriptor<K, V> stateDescriptor) throws Exception {
		return null;
	}

	@Override
	public <S> ListState<S> getListState(
		ListStateDescriptor<S> stateDescriptor) throws Exception {

		RemoteHeapKvStateInfo oldStateInfo = kvStateInformation.get(stateDescriptor.getName());
		TypeSerializer<List<S>> stateSerializer = stateDescriptor.getSerializer();
		TypeSerializer<String> namespaceSerializer = StringSerializer.INSTANCE;
		RemoteHeapKvStateInfo newRedisStateInfo;
		RegisteredKeyValueStateBackendMetaInfo<String, List<S>> newMetaInfo;
		if (oldStateInfo != null) {
			@SuppressWarnings("unchecked")
			String message = String.format(
				"oldStateInfo is not null for RemoteHeapState state Desc %s by %s",
				stateDescriptor.getClass(),
				this.getClass());
			throw new FlinkRuntimeException(message);
		} else {
			newMetaInfo = new RegisteredKeyValueStateBackendMetaInfo<>(
				stateDescriptor.getType(),
				stateDescriptor.getName(),
				namespaceSerializer,
				stateSerializer,
				StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());

			newRedisStateInfo = RedisOperationUtils.createStateInfo(newMetaInfo);
			RedisOperationUtils.registerKvStateInformation(
				this.kvStateInformation,
				stateDescriptor.getName(),
				newRedisStateInfo);
		}

		return RemoteOperatorListState.create(
			stateDescriptor,
			newMetaInfo,
			StateSerializerProvider.fromNewRegisteredSerializer(StringSerializer.INSTANCE).currentSchemaSerializer(),
			this);
	}

	@Override
	public <S> ListState<S> getUnionListState(
		ListStateDescriptor<S> stateDescriptor) throws Exception {
		return null;
	}

	@Override
	public Set<String> getRegisteredStateNames() {
		return null;
	}

	@Override
	public Set<String> getRegisteredBroadcastStateNames() {
		return null;
	}

	@Nonnull
	@Override
	public RunnableFuture<SnapshotResult<OperatorStateHandle>> snapshot(
		long checkpointId,
		long timestamp, @Nonnull CheckpointStreamFactory streamFactory,
		@Nonnull CheckpointOptions checkpointOptions) throws Exception {
		return null;
	}

	public RemoteHeapKvStateInfo getRemoteHeapKvStateInfo(String name) {
		return kvStateInformation.get(name);
	}


}
