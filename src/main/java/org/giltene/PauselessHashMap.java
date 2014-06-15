/*
 * Copyright 2014 Gil Tene.
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Written by Gil Tene, based on Apache Harmony version of java.util.HashMap.
 */
package org.giltene;

import sun.misc.Unsafe;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * PauselessHashMap: A java.util.HashMap compatible Map implementation that
 * performs background resizing for inserts, avoiding the common "resize/rehash"
 * outlier experienced by normal HashMap.
 * <p>
 * get() and put() operations are "pauseless" in the sense that they do not
 * block during resizing of the map. Other operations, like remove(), putAll(),
 * clear(), and the derivation of keysets and such *will* block for pending
 * resize operations.
 * <p>
 * Like HashMap, PauselessHashMap provides no synchronization or thread-safe
 * behaviors on it's own, and MUST be externally synchronized if used by multiple
 * threads. The background resizing mechanism relies on the calling program
 * enforcing serialized access to all methods, and behavior is undefined if
 * concurrent access (for modification or otherwise) is allowed.
 * <p>
 * And like HashMap, PauselessHashMap is an implementation of Map. All optional
 * operations (adding and removing) are supported. Keys and values can be any objects.
 *
 */
public class PauselessHashMap<K, V> extends AbstractMap<K, V> implements Map<K, V>,
        Cloneable, Serializable {

    // If we want to use this as a drop-in for java.util.HashMap, we'll need the following UID:
    // private static final long serialVersionUID = 362498820763181265L;

    // When NOT using as a drop-in, we need our own UID:
    private static final long serialVersionUID = 318739534392781639L;

    /*
     * Actual count of entries
     */
    transient int elementCount;

    /*
     * The internal data structure to hold Entries
     */
    transient Entry<K, V>[] elementData;

    /*
     * The target data structure for holding Entries during resizing:
     */
    transient Entry<K, V>[] resizingIntoElementData;

    /*
     * modification count, to keep track of structural modifications between the
     * HashMap and the iterator
     */
    transient int modCount = 0;

    /*
     * default size that an HashMap created using the default constructor would
     * have.
     */
    private static final int DEFAULT_SIZE = 16;

    /*
     * maximum ratio of (stored elements)/(storage size) which does not lead to
     * rehash
     */
    final float loadFactor;

    /*
     * maximum number of elements that can be put in this map before having to
     * rehash
     */
    int threshold;

    /*
     * pendingResize: A true value indicates a resize has been requested, or is ongoing.
     */
    transient boolean pendingResize = false;

    /**
     * needToKickOffAnotherBackgroundResize: A true value indicates a background resize
     * needs to be kicked off (even if pendingResize is true)
     */
    transient boolean needToKickOffAnotherBackgroundResize = false;

    /*
     * backgroundResizeComplete: A true value indicates that the currently in-progress
     * resizing has completed all background operations.
     */
    transient boolean backgroundResizeComplete = false;   // Worked hard to not use volatile reads for this...

    /*
     * indicatedObservedResizingIntoTable: a non-volatile guard for observedResizingIntoTable.
     * Helps avoid writing to volatiles in the common case.
     */
    transient boolean indicatedObservedResizingIntoTable = false;

    /*
     * observedResizingIntoTable: A true value indicates a put operation has observed the
     * resizingIntoTable allocated by background resizers.
     */
    transient volatile boolean observedResizingIntoTable = false;

    /*
     * volatileUpdateIndicator: An atomic boolean used to perform volatile update actions
     * to enforce ordering (with set, get and lazySet) during background resizing operations.
     */
    transient AtomicBoolean volatileUpdateIndicator = new AtomicBoolean(false);

    /*
     * rehashMonitor: serializes background resizing operations against things that need that sort of thing.
     */
    transient final Object rehashMonitor = new Object();


    /*
     * cachedExpectedHeadsArray: Used to avoid uniqueness scans where possible during background resizing
     */
    transient Entry<K,V>[] cachedExpectedHeadsArray = null;

    // keySet and valuesCollection are taken from Apache Harmony's java.util.AbstractMap. They do
    // Not exist in Java SE version...

    // Lazily initialized key set.
    transient Set<K> keySet;

    transient Collection<V> valuesCollection;

    static class Entry<K, V> extends InternalMapEntry<K, V> {
        final int origKeyHash;
        Entry<K, V> next;
        boolean isValid = true;

        Entry(K theKey, int hash) {
            super(theKey, null);
            this.origKeyHash = hash;
        }

        Entry(K theKey, V theValue) {
            super(theKey, theValue);
            origKeyHash = (theKey == null ? 0 : computeHashCode(theKey));
        }

        void setInvalid() {
            isValid = false;
        }

        void setValid() {
            isValid = true;
        }

        boolean isValid() {
            return isValid;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object clone() {
            Entry<K, V> entry = (Entry<K, V>) super.clone();
            if (next != null) {
                entry.next = (Entry<K, V>) next.clone();
            }
            return entry;
        }
    }


    private static class AbstractMapIterator<K, V>  {
        private int position = 0;
        int expectedModCount;
        Entry<K, V> futureEntry;
        Entry<K, V> currentEntry;
        Entry<K, V> prevEntry;

        final PauselessHashMap<K, V> associatedMap;

        AbstractMapIterator(PauselessHashMap<K, V> hm) {
            associatedMap = hm;
            expectedModCount = hm.modCount;
            futureEntry = null;
            // Make sure any pending resize operation completes before we start this:
            associatedMap.forceFinishResizing();
        }

        public boolean hasNext() {
            if (futureEntry != null) {
                return true;
            }
            while (position < associatedMap.elementData.length) {
                if (associatedMap.elementData[position] == null) {
                    position++;
                } else {
                    return true;
                }
            }
            return false;
        }

        final void checkConcurrentMod() throws ConcurrentModificationException {
            if (associatedMap.resizingIntoElementData != null) {
                throw new ConcurrentModificationException();
            }
            if (expectedModCount != associatedMap.modCount) {
                throw new ConcurrentModificationException();
            }
        }

        @SuppressWarnings("unchecked")
        final void makeNext() {
            checkConcurrentMod();
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            if (futureEntry == null) {
                currentEntry = associatedMap.elementData[position++];
                futureEntry = currentEntry.next;
                prevEntry = null;
            } else {
                if(currentEntry!=null){
                    prevEntry = currentEntry;
                }
                currentEntry = futureEntry;
                futureEntry = futureEntry.next;
            }
        }

        public final void remove() {
            checkConcurrentMod();
            if (currentEntry==null) {
                throw new IllegalStateException();
            }
            if(prevEntry==null){
                int index = currentEntry.origKeyHash & (associatedMap.elementData.length - 1);
                associatedMap.elementData[index] = associatedMap.elementData[index].next;
            } else {
                prevEntry.next = currentEntry.next;
            }
            currentEntry = null;
            expectedModCount++;
            associatedMap.modCount++;
            associatedMap.elementCount--;

        }
    }


    private static class EntryIterator <K, V> extends AbstractMapIterator<K, V> implements Iterator<Map.Entry<K, V>> {

        EntryIterator (PauselessHashMap<K, V> map) {
            super(map);
        }

        public Map.Entry<K, V> next() {
            makeNext();
            return currentEntry;
        }
    }

    private static class KeyIterator <K, V> extends AbstractMapIterator<K, V> implements Iterator<K> {

        KeyIterator (PauselessHashMap<K, V> map) {
            super(map);
        }

        public K next() {
            makeNext();
            return currentEntry.key;
        }
    }

    private static class ValueIterator <K, V> extends AbstractMapIterator<K, V> implements Iterator<V> {

        ValueIterator (PauselessHashMap<K, V> map) {
            super(map);
        }

        public V next() {
            makeNext();
            return currentEntry.value;
        }
    }

    static class HashMapEntrySet<KT, VT> extends AbstractSet<Map.Entry<KT, VT>> {
        private final PauselessHashMap<KT, VT> associatedMap;

        public HashMapEntrySet(PauselessHashMap<KT, VT> hm) {
            associatedMap = hm;
        }

        PauselessHashMap<KT, VT> hashMap() {
            return associatedMap;
        }

        @Override
        public int size() {
            return associatedMap.elementCount;
        }

        @Override
        public void clear() {
            associatedMap.clear();
        }

        @Override
        public boolean remove(Object object) {
            if (!associatedMap.pendingResize) {
                return removeImpl(object);
            }
            synchronized (associatedMap.rehashMonitor) {
                associatedMap.forceFinishResizing();
                return removeImpl(object);
            }
        }

        private boolean removeImpl(Object object) {
            if (object instanceof Map.Entry) {
                Map.Entry<?, ?> oEntry = (Map.Entry<?, ?>) object;
                Entry<KT,VT> entry = associatedMap.getEntry(oEntry.getKey());
                if(valuesEq(entry, oEntry)) {
                    associatedMap.removeEntry(entry);
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean contains(Object object) {
            if (object instanceof Map.Entry) {
                Map.Entry<?, ?> oEntry = (Map.Entry<?, ?>) object;
                Entry<KT, VT> entry = associatedMap.getEntry(oEntry.getKey());
                return valuesEq(entry, oEntry);
            }
            return false;
        }

        private static boolean valuesEq(Entry entry, Map.Entry<?, ?> oEntry) {
            return (entry != null) &&
                    ((entry.value == null) ?
                            (oEntry.getValue() == null) :
                            (areEqualValues(entry.value, oEntry.getValue())));
        }

        @Override
        public Iterator<Map.Entry<KT, VT>> iterator() {
            return new EntryIterator<KT,VT> (associatedMap);
        }
    }

    /**
     * Create a new element array
     *
     * @param s
     * @return Reference to the element array
     */
    @SuppressWarnings("unchecked")
    Entry<K, V>[] newElementArray(int s) {
        return new Entry[s];
    }

    /**
     * Constructs a new empty {@code HashMap} instance.
     */
    public PauselessHashMap() {
        this(DEFAULT_SIZE);
    }

    /**
     * Constructs a new {@code HashMap} instance with the specified capacity.
     *
     * @param capacity
     *            the initial capacity of this hash map.
     * @throws IllegalArgumentException
     *                when the capacity is less than zero.
     */
    public PauselessHashMap(int capacity) {
        this(capacity, 0.75f);  // default load factor of 0.75
    }

    /**
     * Calculates the capacity of storage required for storing given number of
     * elements
     *
     * @param x
     *            number of elements
     * @return storage size
     */
    private static final int calculateCapacity(int x) {
        if(x >= 1 << 30){
            return 1 << 30;
        }
        if(x == 0){
            return 16;
        }
        x = x -1;
        x |= x >> 1;
        x |= x >> 2;
        x |= x >> 4;
        x |= x >> 8;
        x |= x >> 16;
        return x + 1;
    }

    /**
     * Constructs a new {@code HashMap} instance with the specified capacity and
     * load factor.
     *
     * @param capacity
     *            the initial capacity of this hash map.
     * @param loadFactor
     *            the initial load factor.
     * @throws IllegalArgumentException
     *                when the capacity is less than zero or the load factor is
     *                less or equal to zero.
     */
    public PauselessHashMap(int capacity, float loadFactor) {
        if (capacity >= 0 && loadFactor > 0) {
            capacity = calculateCapacity(capacity);
            elementCount = 0;
            elementData = newElementArray(capacity);
            this.loadFactor = loadFactor;
            computeThreshold();
        } else {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Constructs a new {@code HashMap} instance containing the mappings from
     * the specified map.
     *
     * @param map
     *            the mappings to add.
     */
    public PauselessHashMap(Map<? extends K, ? extends V> map) {
        this(calculateCapacity(map.size()));
        putAllImpl(map);
    }

    /**
     * Removes all mappings from this hash map, leaving it empty.
     *
     * @see #isEmpty
     * @see #size
     */
    @Override
    public void clear() {
        if (!pendingResize) {
            clearImpl();
        } else {
            synchronized (rehashMonitor) {
                forceFinishResizing();
                clearImpl();
            }
        }
    }

    private void clearImpl() {
        if (elementCount > 0) {
            elementCount = 0;
            // Arrays.fill(elementData, null);
            for (int i = 0; i < elementData.length; i++) {
                elementData[i] = null;
            }
            modCount++;
        }
    }

    /**
     * Returns a shallow copy of this map.
     *
     * @return a shallow copy of this map.
     */
    @Override
    public Object clone() {
        if (!pendingResize) {
            return cloneImpl();
        } else {
            synchronized (rehashMonitor) {
                forceFinishResizing();
                return cloneImpl();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Object cloneImpl() {
        try {
            PauselessHashMap<K, V> map = (PauselessHashMap<K, V>) super.clone();
            map.keySet = null;
            map.valuesCollection = null;
            map.elementCount = 0;
            map.elementData = newElementArray(elementData.length);
            map.putAll(this);

            return map;
        } catch (CloneNotSupportedException e) {
            return null;
        }
    }

    /**
     * Computes the threshold for rehashing
     */
    private void computeThreshold() {
        Entry<K, V> array[] = (resizingIntoElementData != null) ? resizingIntoElementData : elementData;
        threshold = (int) (array.length * loadFactor);
    }

    /**
     * Returns whether this map contains the specified key.
     *
     * @param key
     *            the key to search for.
     * @return {@code true} if this map contains the specified key,
     *         {@code false} otherwise.
     */
    @Override
    public boolean containsKey(Object key) {
        Entry<K, V> m = getEntry(key);
        return m != null;
    }

    /**
     * Returns whether this map contains the specified value.
     *
     * @param value
     *            the value to search for.
     * @return {@code true} if this map contains the specified value,
     *         {@code false} otherwise.
     */
    @Override
    @SuppressWarnings("unchecked")
    public boolean containsValue(Object value) {
        if (value != null) {
            for (int i = 0; i < elementData.length; i++) {
                Entry<K, V> entry = elementData[i];
                while (entry != null) {
                    if (areEqualValues(value, entry.value)) {
                        return true;
                    }
                    entry = entry.next;
                }
            }
        } else {
            for (int i = 0; i < elementData.length; i++) {
                Entry<K, V> entry = elementData[i];
                while (entry != null) {
                    if (entry.value == null) {
                        return true;
                    }
                    entry = entry.next;
                }
            }
        }

        // Look in the resizing into Entry store, if one exists:
        if (resizingIntoElementData == null)
            return false;

        if (value != null) {
            for (int i = 0; i < resizingIntoElementData.length; i++) {
                Entry<K, V> entry = resizingIntoElementData[i];
                while (entry != null) {
                    if (areEqualValues(value, entry.value)) {
                        return true;
                    }
                    entry = entry.next;
                }
            }
        } else {
            for (int i = 0; i < resizingIntoElementData.length; i++) {
                Entry<K, V> entry = resizingIntoElementData[i];
                while (entry != null) {
                    if (entry.value == null) {
                        return true;
                    }
                    entry = entry.next;
                }
            }
        }

        return false;
    }

    /**
     * Returns a set containing all of the mappings in this map. Each mapping is
     * an instance of {@link java.util.Map.Entry}. As the set is backed by this map,
     * changes in one will be reflected in the other.
     *
     * @return a set of the mappings.
     */
    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return new HashMapEntrySet<K, V>(this);
    }

    /**
     * Returns the value of the mapping with the specified key.
     *
     * @param key
     *            the key.
     * @return the value of the mapping with the specified key, or {@code null}
     *         if no mapping for the specified key is found.
     */
    @Override
    public V get(Object key) {
        Entry<K, V> m = getEntry(key);
        if (m != null) {
            return m.value;
        }
        return null;
    }

    final Entry<K, V> getEntry(Object key) {
        Entry<K, V> m;
        if (key == null) {
            m = findNullKeyEntry();
        } else {
            int hash = computeHashCode(key);
            int index = hash & (elementData.length - 1);
            m = findNonNullKeyEntry(key, index, hash);
        }
        return m;
    }

    final Entry<K,V> findNonNullKeyEntry(Object key, int index, int keyHash) {
        Entry<K,V> m = findNonNullKeyEntryInElementData(key, index, keyHash);
        if (m != null)
            return m;

        // Look in the resizing into Entry store, if one exists:
        if (resizingIntoElementData == null)
            return null;

        int indexInResizedArray = keyHash & (resizingIntoElementData.length - 1);
        return findNonNullKeyEntryInResizingIntoElementData(key, indexInResizedArray, keyHash);
    }

    @SuppressWarnings("unchecked")
    final Entry<K,V> findNonNullKeyEntryInElementData(Object key, int index, int keyHash) {
        return findNonNullKeyEntryInChain(key, elementData[index], keyHash);
    }

    @SuppressWarnings("unchecked")
    final Entry<K,V> findNonNullKeyEntryInResizingIntoElementData(Object key, int index, int keyHash) {
        return findNonNullKeyEntryInChain(key, resizingIntoElementData[index], keyHash);
    }

    final Entry<K,V> findNonNullKeyEntryInChain(Object key, Entry<K,V> chainHead, int keyHash) {
        while (chainHead != null
                && ((!chainHead.isValid()) || chainHead.origKeyHash != keyHash || !areEqualKeys(key, chainHead.key))) {
            chainHead = chainHead.next;
        }
        return chainHead;
    }

    final Entry<K,V> findNullKeyEntry() {
        Entry<K,V> m = findNullKeyEntryInElementData();
        if (m != null)
            return m;

        // Look in the resizing into Entry store, if one exists:
        if (resizingIntoElementData == null)
            return null;
        return findNullKeyEntryInResizingIntoElementData();
    }

    @SuppressWarnings("unchecked")
    final Entry<K,V> findNullKeyEntryInElementData() {
        return findNullKeyEntryInChain(elementData[0]);
    }

    @SuppressWarnings("unchecked")
    final Entry<K,V> findNullKeyEntryInResizingIntoElementData() {
        return findNullKeyEntryInChain(resizingIntoElementData[0]);
    }

    final Entry<K,V> findNullKeyEntryInChain(Entry<K,V> chainHead) {
        while (chainHead != null && ((!chainHead.isValid()) || (chainHead.key != null)))
            chainHead = chainHead.next;
        return chainHead;
    }

    /**
     * Returns whether this map is empty.
     *
     * @return {@code true} if this map has no elements, {@code false}
     *         otherwise.
     * @see #size()
     */
    @Override
    public boolean isEmpty() {
        return elementCount == 0;
    }

    /**
     * Returns a set of the keys contained in this map. The set is backed by
     * this map so changes to one are reflected by the other. The set does not
     * support adding.
     *
     * @return a set of the keys.
     */
    @Override
    public Set<K> keySet() {
        if (keySet == null) {
            keySet = new AbstractSet<K>() {
                @Override
                public boolean contains(Object object) {
                    return containsKey(object);
                }

                @Override
                public int size() {
                    return PauselessHashMap.this.size();
                }

                @Override
                public void clear() {
                    PauselessHashMap.this.clear();
                }

                @Override
                public boolean remove(Object key) {
                    Entry<K, V> entry;
                    if (!pendingResize) {
                        entry = PauselessHashMap.this.removeEntry(key);
                    } else {
                        synchronized (rehashMonitor) {
                            PauselessHashMap.this.forceFinishResizing();
                            entry = PauselessHashMap.this.removeEntry(key);
                        }
                    }
                    return entry != null;
                }

                @Override
                public Iterator<K> iterator() {
                    return new KeyIterator<K,V> (PauselessHashMap.this);
                }
            };
        }
        return keySet;
    }

    /**
     * Maps the specified key to the specified value.
     *
     * @param key
     *            the key.
     * @param value
     *            the value.
     * @return the value of any previous mapping with the specified key or
     *         {@code null} if there was no such mapping.
     */
    @Override
    public V put(K key, V value) {
        return putImpl(key, value);
    }

    private V putImpl(K key, V value) {
        Entry<K,V> entry;
        int index = 0;
        int hash = 0;
        V result = null; // Retain previous value to return to caller

        if(key == null) {
            entry = findNullKeyEntryInElementData();
        } else {
            hash = computeHashCode(key);
            index = hash & (elementData.length - 1);
            entry = findNonNullKeyEntryInElementData(key, index, hash);
        }

        if (resizingIntoElementData == null) {
            if (entry == null) {
                entry = createHashedEntry(key, index, hash);
                entry.value = value;
                modCount++;
                if (++elementCount > threshold) {
                    rehash();
                }
                return null;
            }
            result = entry.value;
            entry.value = value;
            return result;
        }

        // Coordinate with the pending background resize:
        if (!indicatedObservedResizingIntoTable) {
            // Use non-volatile boolean indicator to avoid reading or writing volatile one unless needed:
            indicatedObservedResizingIntoTable = true;
            observedResizingIntoTable = true;
        }

        if (entry != null) {
            // (only) if we found an entry before, update it:
            result = entry.value;
            entry.value = value;

            // If an entry already exists for this key, make sure the key we use from now on
            // is the actual key instance found in the original matching entry, and not just
            // the input key that matches it. This makes sure no new key instances are
            // introduced into the map when non-inserting puts  happen during resizing.
            // If we need to create a new entry in the target array to satisfy the put,
            // we'll want to create it with the same key instance.
            key = entry.key;

            // Need to force a StoreStore fence here to order against next update:
            volatileUpdateIndicator.lazySet(true);
        }

        Entry<K,V> resizingIntoEntry;

        if(key == null) {
            resizingIntoEntry = findNullKeyEntryInResizingIntoElementData();
        } else {
            // No need to re-compute hash here, the prior if (key != null) took care of it.
            // hash = computeHashCode(key);
            index = hash & (resizingIntoElementData.length - 1);
            resizingIntoEntry = findNonNullKeyEntryInResizingIntoElementData(key, index, hash);
        }

        if (resizingIntoEntry == null) {
            resizingIntoEntry = createHashedEntry(key, index, hash);
            if (entry == null) {
                // We should only count this as an actual addition if no entry was
                // found in the elementData scan done earlier. If one was found there,
                // this insertion is simply covering a resizing race, and no actual
                // net new entries are being created.
                modCount++;
                elementCount++;
            }
        } else  if (result == null) {
            // If no result value was retained from previous find, retain this one.
            result = resizingIntoEntry.value;
        }

        resizingIntoEntry.value = value;

        // backgroundResizeComplete is (very intentionally) not volatile, which means that we may
        // "take a while" before noticing a completed background resize operation. E.g., if put
        // does not insert, no forced barriers are sure to occur that would cause us to observe a new
        // backgroundResizeComplete value. However, since any insert (in this state) will use
        // a CAS operation, which has volatile read+write semantics, it will force this thread to
        // re-read backgroundResizeComplete after the insert.
        // We are therefore assured that at most one extra insert will happen before we notice this...
        if (backgroundResizeComplete) {
            finishResizing();
        }

        // Make sure to kick off a resize if needed:
        if (needToKickOffAnotherBackgroundResize) {
            rehash();
        }

        return result;
    }

    void forceFinishResizing() {
        while (pendingResize) {
            try {
                synchronized (rehashMonitor) {
                    if (backgroundResizeComplete) {
                        finishResizing();
                    } else {
                        if (resizingIntoElementData != null) {
                            observedResizingIntoTable = true;
                        }
                        if (needToKickOffAnotherBackgroundResize) {
                            rehash();
                        }
                        // Still in progress or pending, wait for a kick:
                        rehashMonitor.wait(1);
                    }
                }
            } catch (InterruptedException e) {
            }
        }
    }

    void finishResizing() {
        elementData = resizingIntoElementData;
        resizingIntoElementData = null;
        observedResizingIntoTable = false;
        indicatedObservedResizingIntoTable = false;
        backgroundResizeComplete = false;
        pendingResize = false;
        volatileUpdateIndicator.lazySet(false);
        // Kick off another resize if things have accumulated to that level:
        if (elementCount > threshold) {
            rehash();
        }
    }


    @SuppressWarnings("unchecked")
    Entry<K, V> createEntry(K key, int index, V value) {
        Entry<K, V> entry = new Entry<K, V>(key, value);
        entry.next = elementData[index];
        elementData[index] = entry;
        return entry;
    }

    @SuppressWarnings("unchecked")
    Entry<K,V> createHashedEntry(K key, int index, int hash) {
        Entry<K,V> entry = new Entry<K,V>(key,hash);
        if (resizingIntoElementData != null) {
            // We can be racing with a background resize op. So a CAS is needed:
            Entry<K,V> currentHead;
            int indexInResizedArray = hash & (resizingIntoElementData.length - 1);
            do {
                currentHead = resizingIntoElementData[indexInResizedArray];
                entry.next = currentHead;
            } while (!EntryArrayHelper.compareAndSet(resizingIntoElementData, indexInResizedArray, currentHead, entry));

            return entry;
        }
        entry.next = elementData[index];
        elementData[index] = entry;
        return entry;
    }

    /**
     * Copies all the mappings in the specified map to this map. These mappings
     * will replace all mappings that this map had for any of the keys currently
     * in the given map.
     *
     * @param map
     *            the map to copy mappings from.
     * @throws NullPointerException
     *             if {@code map} is {@code null}.
     */
    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        if (!map.isEmpty()) {
            putAllImpl(map);
        }
    }

    private void putAllImpl(Map<? extends K, ? extends V> map) {
        if (resizingIntoElementData != null) {
            if (backgroundResizeComplete) {
                forceFinishResizing();
            }
        }
        int capacity = elementCount + map.size();
        if (capacity > threshold) {
            doResize(capacity);
        }
        for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
            putImpl(entry.getKey(), entry.getValue());
        }
    }

    void rehash(int capacity) {
        if (needToKickOffAnotherBackgroundResize || !pendingResize) {
            kickoffBackgroundResize(capacity);
        }
    }

    void rehash() {
        if (needToKickOffAnotherBackgroundResize || !pendingResize) {
            kickoffBackgroundResize(elementData.length);
        }
        // rehash(elementData.length);
    }

    /**
     * Removes the mapping with the specified key from this map.
     *
     * @param key
     *            the key of the mapping to remove.
     * @return the value of the removed mapping or {@code null} if no mapping
     *         for the specified key was found.
     */
    @Override
    public V remove(Object key) {
        if (!pendingResize) {
            return removeImpl(key);
        }
        synchronized (rehashMonitor) {
            forceFinishResizing();
            return removeImpl(key);
        }
    }

    private V removeImpl(Object key) {
        Entry<K, V> entry = removeEntry(key);
        if (entry != null) {
            return entry.value;
        }
        return null;
    }

    /*
     * Remove the given entry from the hashmap.
     * Assumes that the entry is in the map.
     */

    @SuppressWarnings("unchecked")
    final void removeEntry(Entry<K, V> entry) {
        int index = entry.origKeyHash & (elementData.length - 1);
        Entry<K, V> m = elementData[index];
        if (m == entry) {
            elementData[index] = entry.next;
        } else {
            while ((m != null) && (m.next != entry)) {
                m = m.next;
            }
            if (m != null) {
                m.next = entry.next;
            }
        }
        modCount++;
        elementCount--;
    }

    @SuppressWarnings("unchecked")
    final Entry<K, V> removeEntry(Object key) {
        int index = 0;
        Entry<K, V> entryInElementData;
        Entry<K, V> last = null;
        if (key != null) {
            int hash = computeHashCode(key);
            index = hash & (elementData.length - 1);
            entryInElementData = elementData[index];
            while (entryInElementData != null &&
                    !(entryInElementData.origKeyHash == hash &&
                            areEqualKeys(key, entryInElementData.key))) {
                last = entryInElementData;
                entryInElementData = entryInElementData.next;
            }
        } else {
            entryInElementData = elementData[0];
            while (entryInElementData != null && entryInElementData.key != null) {
                last = entryInElementData;
                entryInElementData = entryInElementData.next;
            }
        }

        if (entryInElementData == null) {
            return null;
        } else {
            if (last == null) {
                elementData[index] = entryInElementData.next;
            } else {
                last.next = entryInElementData.next;
            }
            modCount++;
            elementCount--;
            return entryInElementData;
        }
    }

    /**
     * Returns the number of elements in this map.
     *
     * @return the number of elements in this map.
     */
    @Override
    public int size() {
        return elementCount;
    }

    /**
     * Returns a collection of the values contained in this map. The collection
     * is backed by this map so changes to one are reflected by the other. The
     * collection supports remove, removeAll, retainAll and clear operations,
     * and it does not support add or addAll operations.
     * <p>
     * This method returns a collection which is the subclass of
     * AbstractCollection. The iterator method of this subclass returns a
     * "wrapper object" over the iterator of map's entrySet(). The {@code size}
     * method wraps the map's size method and the {@code contains} method wraps
     * the map's containsValue method.
     * <p>
     * The collection is created when this method is called for the first time
     * and returned in response to all subsequent calls. This method may return
     * different collections when multiple concurrent calls occur, since no
     * synchronization is performed.
     *
     * @return a collection of the values contained in this map.
     */
    @Override
    public Collection<V> values() {
        if (valuesCollection == null) {
            valuesCollection = new AbstractCollection<V>() {
                @Override
                public boolean contains(Object object) {
                    return containsValue(object);
                }

                @Override
                public int size() {
                    return PauselessHashMap.this.size();
                }

                @Override
                public void clear() {
                    PauselessHashMap.this.clear();
                }

                @Override
                public Iterator<V> iterator() {
                    return new ValueIterator<K,V> (PauselessHashMap.this);
                }
            };
        }
        return valuesCollection;
    }

    private void writeObject(ObjectOutputStream stream) throws IOException {
        synchronized (rehashMonitor) {
            forceFinishResizing();
            stream.defaultWriteObject();
            stream.writeInt(elementData.length);
            stream.writeInt(elementCount);
            Iterator<?> iterator = entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<?, ?> entry = (Entry<?, ?>) iterator.next();
                stream.writeObject(entry.key);
                stream.writeObject(entry.value);
                entry = entry.next;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream stream) throws IOException,
            ClassNotFoundException {
        stream.defaultReadObject();
        int length = stream.readInt();
        elementData = newElementArray(length);
        elementCount = stream.readInt();
        for (int i = elementCount; --i >= 0;) {
            K key = (K) stream.readObject();
            int index = (null == key) ? 0 : (computeHashCode(key) & (length - 1));
            createEntry(key, index, (V) stream.readObject());
        }
    }

    /*
     * Contract-related functionality
     */
    static int computeHashCode(Object key) {
        return key.hashCode();
    }

    static boolean areEqualKeys(Object key1, Object key2) {
        return (key1 == key2) || key1.equals(key2);
    }

    static boolean areEqualValues(Object value1, Object value2) {
        return (value1 == value2) || value1.equals(value2);
    }


    // InternalMapEntry was taken from Apache Harmony's java.util.MapEntry (which Java SE does not have).

    static class InternalMapEntry<K, V> implements Map.Entry<K, V>, Cloneable {

        K key;
        V value;

        InternalMapEntry(K theKey) {
            key = theKey;
        }

        InternalMapEntry(K theKey, V theValue) {
            key = theKey;
            value = theValue;
        }

        @Override
        public Object clone() {
            try {
                return super.clone();
            } catch (CloneNotSupportedException e) {
                return null;
            }
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object instanceof Map.Entry) {
                Map.Entry<?, ?> entry = (Map.Entry<?, ?>) object;
                return (key == null ? entry.getKey() == null : key.equals(entry
                        .getKey()))
                        && (value == null ? entry.getValue() == null : value
                        .equals(entry.getValue()));
            }
            return false;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }

        @Override
        public int hashCode() {
            return (key == null ? 0 : key.hashCode())
                    ^ (value == null ? 0 : value.hashCode());
        }

        public V setValue(V object) {
            V result = value;
            value = object;
            return result;
        }

        @Override
        public String toString() {
            return key + "=" + value;
        }
    }

    static class EntryArrayHelper {
        // A static access helper for an Entry[] that uses Unsafe for speed. Provides method compatibility
        // with same-named version aimed for use with AtomicReferenceArray arrays and no Unsafe.

        static final Unsafe UNSAFE;
        static final int objectIndexScale;
        static final int arrayBaseOffset;

        static {
            UNSAFE = getUnsafe();
            objectIndexScale = calculateShiftForScale(UNSAFE.arrayIndexScale(Object[].class));
            arrayBaseOffset = UNSAFE.arrayBaseOffset(Object[].class);

        }

        public static int calculateShiftForScale(final int scale) {
            if (4 == scale) {
                return 2;
            } else if (8 == scale) {
                return 3;
            } else {
                throw new IllegalArgumentException("Unknown pointer size");
            }
        }

        public static Unsafe getUnsafe() {
            try {
                Field f = Unsafe.class.getDeclaredField("theUnsafe");
                f.setAccessible(true);
                return (Unsafe)f.get(null);
            } catch (Exception e) {
            }
            return null;
        }

        static boolean compareAndSet(Entry[] array, int index, Entry expected, Entry entry) {
            long offset = arrayBaseOffset + (index << objectIndexScale);
            return UNSAFE.compareAndSwapObject(array, offset, expected, entry);
        }
    }

    class Resizer implements Runnable {
        final PauselessHashMap<K, V> associatedMap;
        final int capacity;
        final boolean isSynchronous;
        long startTimeNsec = 0;

        Resizer(PauselessHashMap<K, V> hm, int capacity, boolean isSynchronous) {
            associatedMap = hm;
            this.capacity = capacity;
            this.isSynchronous = isSynchronous;
        }

        void insertUniqueEquivalentAtHeadOfBucket(Entry<K, V> existingEntry, int bucketIndex) {
            K key = existingEntry.key;
            int keyHash = existingEntry.origKeyHash;
            Entry<K, V> newEntry = new Entry<K, V>(key, existingEntry.value);
            newEntry.setInvalid();
            boolean keyFound = false;

            Entry<K,V> targetBucketHead;
            do {
                targetBucketHead = resizingIntoElementData[bucketIndex];
                newEntry.next = targetBucketHead;

                if (!isSynchronous) {
                    // Verify the entry will be unique in the target bucket:

                    if (cachedExpectedHeadsArray[bucketIndex] != targetBucketHead) {
                        // If the head does is not the one we expect it to be (i.e. null or
                        // the one we put in there before) then the mutator has inserted
                        // something, and we need to actually scan the bucket to verify
                        // that an entry with the same key has not been inserted:

                        if (key == null) {
                            keyFound = (findNullKeyEntryInChain(targetBucketHead) != null);
                        } else {
                            keyFound = (findNonNullKeyEntryInChain(key, targetBucketHead, keyHash) != null);
                        }

                        // This key is already in there, and that entry takes precedence:
                        if (keyFound)
                            return;
                    }
                }

                // We are potentially racing against a putting mutator, so a CAS is needed:
            } while (!EntryArrayHelper.compareAndSet(resizingIntoElementData, bucketIndex, targetBucketHead, newEntry));

            if (!isSynchronous) {
                // Cache this entry as the expected head for this bucket:
                cachedExpectedHeadsArray[bucketIndex] = newEntry;
            }

            // there is a StoreStore fence ahead of here due to the CAS above

            // From now on, puts to existing entry will be sure to change both entries
            newEntry.value = existingEntry.value; // Existing entry MAY have been changed by racing put() call.

            // Force a StoreStore fence:
            volatileUpdateIndicator.lazySet(true);

            newEntry.setValid();
        }

        @SuppressWarnings("unchecked")
        public void run() {
            if (startTimeNsec == 0) {
                startTimeNsec = System.nanoTime();
            }

            synchronized (rehashMonitor) {
                boolean boolval;

                // Fast forward to make room for at least the current count. Useful for cases
                // where inserts are happening so quickly that the capacity used to trigger
                // the resizer is stale by the time we get here...
                int needed_capacity  = Math.max(capacity, (int)(elementCount / loadFactor));

                needed_capacity = calculateCapacity((capacity == 0 ? 1 : needed_capacity << 1));

                if (resizingIntoElementData == null) {
                    resizingIntoElementData = newElementArray(needed_capacity);
                    if (!isSynchronous) {
                        cachedExpectedHeadsArray = newElementArray(needed_capacity);
                    }
                }

                int length = resizingIntoElementData.length;

                // Force a full fence:
                boolval = volatileUpdateIndicator.get();
                volatileUpdateIndicator.set(!boolval);

                if (isSynchronous) {
                    observedResizingIntoTable = true;
                }

                if (!observedResizingIntoTable) {
                    // This is effectively a spin-wait until the mutator has signaled that it has
                    // observed the new Element array.

                    if ((System.nanoTime() - startTimeNsec) > NSEC_TIME_TO_SPINWAIT_IN_RESIZER_BEFORE_GIVING_UP) {
                        // stop spinning, and instead make a future mutator op kick off another resizer.
                        needToKickOffAnotherBackgroundResize = true;
                        return;
                    }

                    // We spin wait by scheduling ourselves for a new execution so as not to block other
                    // users of the executors thread pool.
                    backgroundResizers.schedule(this, 1, TimeUnit.MILLISECONDS);

                    return;
                }

                for (int i = 0; i < elementData.length; i++) {
                    Entry<K, V> existingEntry = elementData[i];

                    while (existingEntry != null) {
                        int bucketIndex = existingEntry.origKeyHash & (length - 1);

                        insertUniqueEquivalentAtHeadOfBucket(existingEntry, bucketIndex);

                        existingEntry = existingEntry.next;
                    }
                    elementData[i] = null;
                }

                // Force a StoreStore fence:
                volatileUpdateIndicator.lazySet(!boolval);

                // We don't need the expected head cache any more:
                cachedExpectedHeadsArray = null;

                computeThreshold();
                backgroundResizeComplete = true;
                rehashMonitor.notifyAll(); // Kick anyone waiting for the resize to complete...
            }
        }
    }

    static final long NSEC_TIME_TO_SPINWAIT_IN_RESIZER_BEFORE_GIVING_UP = 100 * 1000L * 1000L;

    static final int DEFAULT_NUMBER_OF_BACKGROUND_RESIZER_EXECUTOR_THREADS = 2;

    static final int SMALLEST_CAPACITY_TO_KICK_OFF_BACKGROUND_RESIZE_FOR = 256;

    static final AtomicInteger backgroundResizerthreadNumber = new AtomicInteger(0);

    static final ScheduledExecutorService backgroundResizers =
            Executors.newScheduledThreadPool(DEFAULT_NUMBER_OF_BACKGROUND_RESIZER_EXECUTOR_THREADS,
                    new ThreadFactory() {
                        @Override
                        public Thread newThread(Runnable runnable) {
                            Thread thread = new Thread(runnable);
                            thread.setName("PauselessHashMap-resizer-pool-thread-" +
                                    backgroundResizerthreadNumber.getAndIncrement());
                            thread.setDaemon(true);
                            return thread;
                        }
                    });

    final void kickoffBackgroundResize(int capacity) {

        if (capacity < SMALLEST_CAPACITY_TO_KICK_OFF_BACKGROUND_RESIZE_FOR) {
            doResize(capacity);
            return;
        }

        // Big enough to do in the background:
        pendingResize = true;
        needToKickOffAnotherBackgroundResize = false;
        backgroundResizers.execute(new Resizer(this, capacity, false /* non-synchronous */));

        /* the following commented out code is here to play games with timing, and make bugs
         * happen (or not) more or less often. It is going away once all the kinks are
         * worked out ;-)
         */

//        // Loop to observe new array. Helps bring background resizers to the right spot and
//        // induces more concurrency (but also avoids potential races around first observation):
//
//        while (resizingIntoElementData == null) {
//            volatileUpdateIndicator.set(!volatileUpdateIndicator.get());
//        }
//        observedResizingIntoTable = true;
//        volatileUpdateIndicator.set(!volatileUpdateIndicator.get());

//        // Sleep to give the background resizers time to finish and avoid concurrency
//        // This is "surprisingly" useful for passing tests ;-).
//        try {
//            TimeUnit.MILLISECONDS.sleep(20);
//        } catch (InterruptedException ex) {
//
//        }
    }

    final void doResize(int capacity) {
        Resizer resizer = new Resizer(this, capacity, true /* synchronous */);
        pendingResize = true;
        synchronized (rehashMonitor) {
            resizer.run();
            // Synchronously finish resizing to avoid having others deal with it:
            finishResizing();
        }
    }

}

