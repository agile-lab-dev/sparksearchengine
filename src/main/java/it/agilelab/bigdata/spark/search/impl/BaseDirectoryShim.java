package it.agilelab.bigdata.spark.search.impl;

import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.lucene.store.LockFactory;

/**
 * This shim class exists only to allow serialization of its subclasses, as the
 * serialization framework needs a no-arg constructor from a parent class to
 * call while creating the object.
 */
public abstract class BaseDirectoryShim extends BaseDirectory {	
	protected BaseDirectoryShim() {
		super(new SingleInstanceLockFactory());
	}
	
	protected BaseDirectoryShim(LockFactory lockFactory) {
		super(lockFactory);
	}
}