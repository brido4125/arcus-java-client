/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package net.spy.memcached;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Collection;

import net.spy.memcached.ops.Operation;

class MemcachedNodeROImpl implements MemcachedNode {

	private final MemcachedNode root;

	public MemcachedNodeROImpl(MemcachedNode n) {
		super();
		root=n;
	}

	@Override
	public String toString() {
		return root.toString();
	}

	public void addOp(Operation op) {
		throw new UnsupportedOperationException();
	}

	public void insertOp(Operation op) {
		throw new UnsupportedOperationException();
	}

	public void connected() {
		throw new UnsupportedOperationException();
	}

	public void copyInputQueue() {
		throw new UnsupportedOperationException();
	}

	public void fillWriteBuffer(boolean optimizeGets) {
		throw new UnsupportedOperationException();
	}

	public void fixupOps() {
		throw new UnsupportedOperationException();
	}

	public int getBytesRemainingToWrite() {
		return root.getBytesRemainingToWrite();
	}

	public SocketChannel getChannel() {
		throw new UnsupportedOperationException();
	}

	public Operation getCurrentReadOp() {
		throw new UnsupportedOperationException();
	}

	public Operation getCurrentWriteOp() {
		throw new UnsupportedOperationException();
	}

	public ByteBuffer getRbuf() {
		throw new UnsupportedOperationException();
	}

	public int getReconnectCount() {
		return root.getReconnectCount();
	}

	public int getSelectionOps() {
		return root.getSelectionOps();
	}

	public SelectionKey getSk() {
		throw new UnsupportedOperationException();
	}

	public SocketAddress getSocketAddress() {
		return root.getSocketAddress();
	}

	public ByteBuffer getWbuf() {
		throw new UnsupportedOperationException();
	}

	public boolean hasReadOp() {
		return root.hasReadOp();
	}

	public boolean hasWriteOp() {
		return root.hasReadOp();
	}

	public boolean isActive() {
		return root.isActive();
	}

	public void reconnecting() {
		throw new UnsupportedOperationException();
	}

	public void registerChannel(SocketChannel ch, SelectionKey selectionKey) {
		throw new UnsupportedOperationException();
	}

	public Operation removeCurrentReadOp() {
		throw new UnsupportedOperationException();
	}

	public Operation removeCurrentWriteOp() {
		throw new UnsupportedOperationException();
	}

	public void setChannel(SocketChannel to) {
		throw new UnsupportedOperationException();
	}

	public void setSk(SelectionKey to) {
		throw new UnsupportedOperationException();
	}

	public void setupResend(boolean cancelWrite) {
		throw new UnsupportedOperationException();
	}

	public void transitionWriteItem() {
		throw new UnsupportedOperationException();
	}

	public int writeSome() throws IOException {
		throw new UnsupportedOperationException();
	}

	public Collection<Operation> destroyInputQueue() {
		throw new UnsupportedOperationException();
	}

	public void authComplete() {
		throw new UnsupportedOperationException();
	}

	public void setupForAuth() {
		throw new UnsupportedOperationException();
	}

	public int getContinuousTimeout() {
		throw new UnsupportedOperationException();
	}

	public void setContinuousTimeout(boolean isIncrease) {
		throw new UnsupportedOperationException();
	}
	
	public boolean isFake() {
		throw new UnsupportedOperationException(); 
	}

	public void shutdown() throws IOException {
		throw new UnsupportedOperationException();
	}
	
	public String getStatus() {
		throw new UnsupportedOperationException();
	}
}
