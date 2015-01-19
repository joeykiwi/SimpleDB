package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator m_child;
    private TransactionId m_tid;
    private boolean m_done;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	m_tid = t;
    	m_child = child;
    	m_done = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	Type[] types = new Type[1];
    	types[0] = Type.INT_TYPE;
    	String[] names = new String[1];
    	names[0] = "delete_count";
        return new TupleDesc(types, names);
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	m_child.open();
    }

    public void close() {
        // some code goes here
    	super.close();
    	m_child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	m_child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (m_done)
    		return null;
    	
    	int delete_count = 0;
    	try {
    		while (m_child.hasNext()) {
    			Tuple next = m_child.next();
    			delete_count++;
    			Database.getBufferPool().deleteTuple(m_tid, next);
    		}
    	} catch(IOException e) {
    		System.err.println("Error inserting tuple");
    		System.exit(1);
    	}
    	
    	IntField intField = new IntField(delete_count);
    	Tuple result = new Tuple(getTupleDesc());
    	result.setField(0, intField);
    	m_done = true;
    	return result;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	DbIterator[] result = new DbIterator[1];
    	result[0] = m_child;
        return result;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	m_child = children[0];
    }

}
