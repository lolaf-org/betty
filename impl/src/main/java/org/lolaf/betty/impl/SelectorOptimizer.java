/*
 * Copyright © 2024-2026 Lolaf.org
 *
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
package org.lolaf.betty.impl;

import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.lolaf.ringos.unsafe.UnsafeOperations;
import org.lolaf.ringos.unsafe.UnsafeOperationsApi;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.*;
import java.util.function.Consumer;

@Slf4j
class SelectorOptimizer {

    public static SelectorPair openSelector(SelectorProvider provider, IOWorkerImpl ioWorker) throws IOException {
        Selector unwrappedSelector = provider.openSelector();

        if (!UnsafeOperationsApi.isAvailable()) {
            try {
                UnsafeOperationsApi.get();
            } catch (RuntimeException e) {
                log.warn("Unable to setup selector optimization, UnsafeOperationsApi is not available: {}", e.getMessage());
            }
            return new SelectorPair(unwrappedSelector, unwrappedSelector);
        }

        // No AccessController.doPrivileged around this: the security manager has been permanently disabled
        // since Java 24 (JEP 486), so doPrivileged only ever ran the action inline, and on the JDKs that still
        // accept one this code already took the unprivileged path whenever none was installed.
        Class<?> selectorImplClass;
        try {
            selectorImplClass = Class.forName("sun.nio.ch.SelectorImpl", false, ClassLoader.getSystemClassLoader());
        } catch (Throwable cause) {
            log.warn("Failed to setup selector optimization", cause);
            return new SelectorPair(unwrappedSelector, unwrappedSelector);
        }
        if (!selectorImplClass.isAssignableFrom(unwrappedSelector.getClass())) {
            return new SelectorPair(unwrappedSelector, unwrappedSelector);
        }

        OptimizedSelectionKeySet selectedKeySet = new OptimizedSelectionKeySet();

        UnsafeOperations unsafe = UnsafeOperationsApi.get();
        long selectedKeysFieldOffset = unsafe.objectFieldOffset(selectorImplClass, "selectedKeys");
        long publicSelectedKeysFieldOffset = unsafe.objectFieldOffset(selectorImplClass, "publicSelectedKeys");
        if (selectedKeysFieldOffset != UnsafeOperations.UNKNOWN_FIELD_OFFSET
                && publicSelectedKeysFieldOffset != UnsafeOperations.UNKNOWN_FIELD_OFFSET) {
            unsafe.putReference(unwrappedSelector, selectedKeysFieldOffset, selectedKeySet);
            unsafe.putReference(unwrappedSelector, publicSelectedKeysFieldOffset, selectedKeySet);
            log.info("NIO selector optimization enabled for {}", ioWorker.getName());
        } else {
            log.warn("Failed to setup selector optimization");
        }
        return new SelectorPair(unwrappedSelector, new OptimizedSelector(unwrappedSelector, selectedKeySet));
    }

    private static final class OptimizedSelectionKeySet extends AbstractSet<SelectionKey> implements Iterator<SelectionKey> {
        // should match the number of live connections per io thread, will grow anyway
        private static final int DEFAULT_SELECTION_KEY_ARRAY_SIZE =
                Integer.parseInt(System.getProperty("OptimizedSelectionKeySet.default.array.size", "32"));

        private SelectionKey[] keys;
        private int size;
        private int iteratorIndex;

        OptimizedSelectionKeySet() {
            keys = new SelectionKey[DEFAULT_SELECTION_KEY_ARRAY_SIZE];
        }

        @Override
        public boolean add(SelectionKey selectionKey) {
            if (selectionKey == null) {
                return false;
            }
            if (size == keys.length) {
                increaseCapacity();
            }
            keys[size++] = selectionKey;
            return true;
        }

        @Override
        public boolean remove(Object selectionKey) {
            for (int i = 0; i < size; i++) {
                if (keys[i] == selectionKey) {
                    keys[i] = null;
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean contains(Object o) {
            SelectionKey[] array = keys;
            for (int i = 0, s = size; i < s; i++) {
                SelectionKey k = array[i];
                if (k.equals(o)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean hasNext() {
            return iteratorIndex < size;
        }

        @Override
        public SelectionKey next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return keys[iteratorIndex++];
        }

        @Override
        public void remove() {
            keys[iteratorIndex - 1] = null;
        }

        @Override
        public Iterator<SelectionKey> iterator() {
            // no multithreaded access so no risks
            iteratorIndex = 0;
            return this;
        }

        void reset() {
            if (keys[0] != null) {
                Arrays.fill(keys, 0, size, null);
            }
            size = 0;
        }

        private void increaseCapacity() {
            SelectionKey[] newKeys = new SelectionKey[keys.length << 1];
            log.debug("Resizing optimized selector selection keys capacity from {} to {}", keys.length, newKeys.length);
            System.arraycopy(keys, 0, newKeys, 0, size);
            keys = newKeys;
        }
    }

    private static final class OptimizedSelector extends Selector {
        private final OptimizedSelectionKeySet selectionKeys;
        private final Selector delegate;

        OptimizedSelector(Selector delegate, OptimizedSelectionKeySet selectionKeys) {
            this.delegate = delegate;
            this.selectionKeys = selectionKeys;
        }

        @Override
        public boolean isOpen() {
            return delegate.isOpen();
        }

        @Override
        public SelectorProvider provider() {
            return delegate.provider();
        }

        @Override
        public Set<SelectionKey> keys() {
            return delegate.keys();
        }

        @Override
        public Set<SelectionKey> selectedKeys() {
            return delegate.selectedKeys();
        }

        @Override
        public int selectNow() throws IOException {
            selectionKeys.reset();
            return delegate.selectNow();
        }

        @Override
        public int select(long timeout) throws IOException {
            selectionKeys.reset();
            return delegate.select(timeout);
        }

        @Override
        public int select() throws IOException {
            selectionKeys.reset();
            return delegate.select();
        }

        @Override
        public int select(Consumer<SelectionKey> action) throws IOException {
            selectionKeys.reset();
            return delegate.select(action);
        }

        @Override
        public int select(Consumer<SelectionKey> action, long timeout) throws IOException {
            selectionKeys.reset();
            return delegate.select(action, timeout);
        }

        @Override
        public int selectNow(Consumer<SelectionKey> action) throws IOException {
            selectionKeys.reset();
            return delegate.selectNow(action);
        }

        @Override
        public Selector wakeup() {
            return delegate.wakeup();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    @Value
    public static class SelectorPair {
        Selector selector;
        Selector optimizedSelector;
    }
}