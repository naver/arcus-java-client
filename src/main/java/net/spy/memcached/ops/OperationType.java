package net.spy.memcached.ops;

public enum OperationType {
	/* 
	 * only Ascii Operation have WRITE or READ, ETC OperationType
	 * Binary Operation has only UNDEFIED OperationType
	 * because Arcus Client don't use Binary Operation.
	 */
	
	/*
	 * StoreOperationImpl (add / replace / set / asyncSetBulk)
	 * ConcatenationOperationImpl (append / prepend)
	 * MutatorOperationImpl (asyncIncr / asyncDecr)
	 * DeleteOperationImpl (delete)
	 * CASOperationImpl (asyncCAS, cas)
	 * CollectionCreateOperationImpl (asyncBopCreate / asyncLopCreate / asyncSopCreate)
	 * CollectionStoreOperationImpl (asyncBopInsert / asyncLopInsert / asyncSopInsert)
	 * CollectionDeleteOperationImpl (asynBopDelete / asyncLopDelete / asyncSopDelete)
	 * CollectionGetOperationImpl (asyncBopGet / asyncLopGet / asyncSopGet) is WRITE, when withDelete is true.
	 * ExtendedBTreeGetOperationImpl (asyncBopGet) is WRITE, when withDelete is true
	 * CollectionBulkStoreOperationImpl (asyncBopInsertBulk / asyncLopInsertBulk / asyncSopInsertBulk) 
	 * CollectionPipedStoreOperationImpl (asyncBopPipedInsertBulk / asyncLopPipedInsertBulk / asyncSopPipedInsertBulk)
	 * CollectionPipedUpdateOperationImpl (asyncBopPipedUpdateBulk)
	 * CollectionUpsertOperationImpl (asyncBopUpsert)
	 * CollectionUpdateOperationImpl (asyncBopUpdate)
	 * CollectionMutateOperationImpl (asyncBopIncr / asyncBopDecr)
	 * BtreeStoreAndGetOpeartionImpl (asynBopInsertAndGetTrimmed)
	 * FlushOperationImpl / FlushByPrefixOperationImpl (flush)
	 * SetAttrOperationImpl (asyncSetAttr)
	 */
	WRITE,
	
	/*
	 * GetOperationImpl (asyncGet / asyncGetBulk)
	 * GetsOperationImpl (asyncGets)
	 * CollectionGetOperationImpl (asynBopGet / asyncLopGet / asyncSopGet) is READ, when withDelete is false.
	 * ExtendedBTreeGetOperationImpl (asyncBopGet) is READ, when withDelete is false.
	 * CollectionExistOperationImpl (asyncSopExist)
	 * CollectionPipedExistOperationImpl (asyncSopPipedExistBulk)
	 * CollectionCountOperationImpl (asyncBopGetItemCount)
	 * BTreeGetBulkOperationImpl (asyncBopGetBulk)
	 * BTreeSortMergeGetOperationImpl (asyncBopSortMergeGet)
	 * BTreeFindPositionOperationImpl (asyncBopFindPosition)
	 * BTreeGetByPositionOperationImpl (asyncBopGetByPosition)
	 * BTreeFindPositionWithGetOperationImpl (asyncBopFindPositionWithGet)
	 * GetAttrOperationImpl (asyncGetAttr)
	 */
	READ,
	
	/* for Collection Get API (with delete, dropIfEmpty) */
	RW,
	
	/*
	 * StatsOperationImpl (getStats)
	 * VersionOperationImpl (getVersions) 
	 */
	ETC,
	
	/*
	 * When the type of an operation is undefined,
	 * it has the operation type of UNDEFINED.
	 */
	UNDEFINED
}

/* Operation Hierarchy (only ascii type)
 * ├── BaseOperationImple.java-Abstract
   └── Operation.java-Interface
       ├── OperationImpl.java-extends[BaseOperationImpl]-implements[Operation]
       │   ├── BaseGetOpImpl.java-Abstract-extends[OperationImpl]
       │   └── BaseStoreOperationImpl.java-Abstract-extends[OperationImpl]
       ├── FlushOperation.java-Interface-extends[Operation]
       │   ├── FlushByPrefixOperationImpl.java-extends[OperationImpl]-implements[FlushOperation]
       │   └── FlushOperationImpl.java-extends[OperationImpl]-implements[FlushOperation]
       ├── KeyedOperation.java-Interface-extends[Operation]
       │   ├── BTreeFindPositionOperation.java-Interface-extends[KeyedOperation]
       │   │   └── BTreeFindPositionOperationImpl.java-extends[OperationImpl]-implements[BTreeFindPositionOperation]
       │   ├── BTreeFindPositionWithGetOperation.java-Interface-extends[KeyedOperation]
       │   │   └── BTreeFindPositionWithGetOperationImpl.java-extends[OperationImpl]-implements[KeyedOpeartion]
       │   ├── BTreeGetBulkOperation.java-Interface-extends[KeyedOperation]
       │   │   └── BTreeGetBulkOperationImpl.java-extends[OpeartionImpl]-implements[KeyedOpeartion]
       │   ├── BTreeGetByPositionOperation.java-Interface-extends[KeyedOperation]
       │   │   └── BTreeGetByPositionOperationImpl.java-extends[OperationImpl]-implements[KeyedOpeartion]
       │   ├── BTreeSortMergeGetOperation.java-Interface-extends[KeyedOpeartion]
       │   │   └── BTreeSortMergeGetOperationImpl.java-extends[OperationImpl]-implements[BtreeSortMergeGetOperation]
       │   ├── BTreeStoreAndGetOperation.java-Interface-extends[KeyedOperation]
       │   │   └── BTreeStoreAndGetOperationImpl.java-extends[OperationImpl]-implements[BtreeStoreAndGetOperation]
       │   ├── CASOperation.java-Interface-extends[KeyedOperation]
       │   │   └── CASOperationImpl.java-extends[OpeartionImpl]-implements[CASOperation]
       │   ├── CollectionBulkStoreOperation.java-Interface-extends[KeyedOperation]
       │   │   └── CollectionBulkStoreOperationImpl.java-extends[OpeartionImpl]-implements[CollectionBulkStoreOperation]
       │   ├── CollectionCountOperation.java-Interface-extends[KeyedOperation]
       │   │   └── CollectionCountOperationImpl.java-extends[OperationImpl]-implements[CollectionCountOperation]
       │   ├── CollectionCreateOperation.java-Interface-extends[KeyedOperation]
       │   │   └── CollectionCreateOperationImpl.java-extends[OperationImpl]-implements[CollectionCreateOperation]
       │   ├── CollectionDeleteOperation.java-Interface-extends[KeyedOperation]
       │   │   └── CollectionDeleteOperationImpl.java-extends[OperationImpl]-implements[CollectionDeleteOperation]
       │   ├── CollectionExistOperation.java-Interface-extends[KeyedOperation]
       │   │   └── CollectionExistOperationImpl.java-extends[OperationImpl]-implements[CollectionExistOperation]
       │   ├── CollectionGetOperation.java-Interface-extends[KeyedOperation]
       │   │   ├── CollectionGetOperationImpl.java-extends[OperationImpl]-implements[CollectionGetOperation]
       │   │   └── ExtendedBTreeGetOperationImpl.java-extends[OperationImpl]-implements[CollectionGetOperation]
       │   ├── CollectionMutateOperation.java-Interface-extends[KeyedOperation]
       │   │   └── CollectionMutateOperationImpl.java-extends[OperationImpl]-implements[CollectionMutateOperation]
       │   ├── CollectionPipedExistOperation.java-Interface-extends[KeyedOperation]
       │   │   └── CollectionPipedExistOperationImpl.java-extends[OperationImpl]-implements[CollectionPipedExist]
       │   ├── CollectionPipedStoreOperation.java-Interface-extends[KeyedOperation]
       │   │   └── CollectionPipedStoreOperationImpl.java-extends[OperationImpl]-implements[CollectionPipedStore]
       │   ├── CollectionPipedUpdateOperation.java-Interface-extends[KeyedOperation]
       │   │   └── CollectionPipedUpdateOperationImpl.java-extends[OperationImpl]-implements[CollectionPipedUpdateOperation]
       │   ├── CollectionStoreOperation.java-Interface-extends[KeyedOperation]
       │   │   ├── CollectionStoreOperationImpl.java-extends[OperationImpl]-implements[CollectionStoreOperation]
       │   │   └── CollectionUpsertOperationImpl.java-extends[OpeartionImpl]-implements[CollectionStoreOperation]
       │   ├── CollectionUpdateOperation.java-Interface-extends[KeyedOperation]
       │   │   └── CollectionUpdateOperationImpl.java-extends[OperationImpl]-implements[OperationUpdateOperation]
       │   ├── CollectionUpsertOperation.java-Interface-extends[KeyedOperation]
       │   ├── ConcatenationOperation.java-Interface-extends[KeyedOperation]
       │   │   └── ConcatenationOperationImpl.java-extends[BaseStoreOperationImpl]-implements[ConcatenationOperation]
       │   ├── DeleteOperation.java-Interface-extends[KeyedOperation]
       │   │   └── DeleteOperationImpl.java-extends[OperationImpl]-implements[DeleteOperation]
       │   ├── ExtendedBTreeGetOperation.java-Interface-extends[KeyedOperation]
       │   ├── GetAttrOperation.java-Interface-extends[KeyedOperation]
       │   │   └── GetAttrOperationImpl.java-extends[OperationImpl]-implements[GetAttrOperation]
       │   │       └── OptimizedGetImpl.java-extends[GetOperationImpl]
       │   ├── GetOperation.java-Interface-extends[KeyedOperation]
       │   │   └── GetOperationImpl.java-extends[BaseGetOpImpl]-implements[GetOperation]
       │   ├── GetsOperation.java-Interface-extends[KeyedOperation]
       │   │   └── GetsOperationImpl.java-extends[BaseGetOpImpl]-implements[GetsOperation]
       │   ├── MutatorOperation.java-Interface-extends[KeyedOperation]
       │   │   └── MutatorOperationImpl.java-extends[OperationImpl]-implements[MutatorOperation]
       │   ├── SetAttrOperation.java-Interface-extends[KeyedOperation]
       │   │   └── SetAttrOperationImpl.java-extends[OperationImpl]-implements[SetAttrOperation]
       │   └── StoreOperation.java-Interface-extends[KeyedOperation]
       │   │   └── StoreOperationImpl.java-extends[BaseStoreOperationImpl]-implements[StoreOperation]
       ├── StatsOperation.java-Interface-extends[Operation]
       │   └── StatsOperationImpl.java-extends[OperationImpl]-implements[StatsOperation]
       ├── NoopOperation.java-Interface-extends[Operation]
       └── VersionOperation.java-Interface-extends[Operation]
           └── VersionOperationImpl.java-extends[OperationImpl]-implements[VersionOperation]-implements[NoopOperation]
 */