/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache.entries;

// DO NOT modify this class. It was generated from LeafRegionEntry.cpp
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.cache.lru.EnableLRU;
import org.apache.geode.internal.cache.DiskId;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.PlaceHolderDiskRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;
import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap.HashEntry;

// macros whose definition changes this class:
// disk: DISK
// lru: LRU
// stats: STATS
// versioned: VERSIONED
// offheap: OFFHEAP
// One of the following key macros must be defined:
// key object: KEY_OBJECT
// key int: KEY_INT
// key long: KEY_LONG
// key uuid: KEY_UUID
// key string1: KEY_STRING1
// key string2: KEY_STRING2
/**
 * Do not modify this class. It was generated. Instead modify LeafRegionEntry.cpp and then run
 * ./dev-tools/generateRegionEntryClasses.sh (it must be run from the top level directory).
 */
public class VMThinDiskRegionEntryHeapIntKey extends VMThinDiskRegionEntryHeap {
  public VMThinDiskRegionEntryHeapIntKey(RegionEntryContext context, int key, Object value) {
    super(context, (value instanceof RecoveredEntry ? null : value));
    // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
    initialize(context, value);
    this.key = key;
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  // common code
  protected int hash;
  private HashEntry<Object, Object> next;
  @SuppressWarnings("unused")
  private volatile long lastModified;
  private static final AtomicLongFieldUpdater<VMThinDiskRegionEntryHeapIntKey> lastModifiedUpdater =
      AtomicLongFieldUpdater.newUpdater(VMThinDiskRegionEntryHeapIntKey.class, "lastModified");
  private volatile Object value;

  @Override
  protected Object getValueField() {
    return this.value;
  }

  @Override
  protected void setValueField(Object v) {
    this.value = v;
  }

  protected long getLastModifiedField() {
    return lastModifiedUpdater.get(this);
  }

  protected boolean compareAndSetLastModifiedField(long expectedValue, long newValue) {
    return lastModifiedUpdater.compareAndSet(this, expectedValue, newValue);
  }

  /**
   * @see HashEntry#getEntryHash()
   */
  public int getEntryHash() {
    return this.hash;
  }

  protected void setEntryHash(int v) {
    this.hash = v;
  }

  /**
   * @see HashEntry#getNextEntry()
   */
  public HashEntry<Object, Object> getNextEntry() {
    return this.next;
  }

  /**
   * @see HashEntry#setNextEntry
   */
  public void setNextEntry(final HashEntry<Object, Object> n) {
    this.next = n;
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  // disk code
  protected void initialize(RegionEntryContext context, Object value) {
    diskInitialize(context, value);
  }

  @Override
  public int updateAsyncEntrySize(EnableLRU capacityController) {
    throw new IllegalStateException("should never be called");
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  private void diskInitialize(RegionEntryContext context, Object value) {
    DiskRecoveryStore drs = (DiskRecoveryStore) context;
    DiskStoreImpl ds = drs.getDiskStore();
    long maxOplogSize = ds.getMaxOplogSize();
    // get appropriate instance of DiskId implementation based on maxOplogSize
    this.id = DiskId.createDiskId(maxOplogSize, true/* is persistence */, ds.needsLinkedList());
    Helper.initialize(this, drs, value);
  }

  /**
   * DiskId
   * 
   * @since GemFire 5.1
   */
  protected DiskId id;// = new DiskId();

  public DiskId getDiskId() {
    return this.id;
  }

  @Override
  public void setDiskId(RegionEntry old) {
    this.id = ((AbstractDiskRegionEntry) old).getDiskId();
  }

  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
  // key code
  private final int key;

  @Override
  public Object getKey() {
    return this.key;
  }

  @Override
  public boolean isKeyEqual(Object k) {
    if (k instanceof Integer) {
      return ((Integer) k).intValue() == this.key;
    }
    return false;
  }
  // DO NOT modify this class. It was generated from LeafRegionEntry.cpp
}
