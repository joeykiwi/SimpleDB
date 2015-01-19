package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator m_child;
    private int m_tabId;
    private TransactionId m_tid;
    private boolean m_done;
    
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
    	m_tabId = tableid;
    	m_child = child;
    	m_tid = t;
    	m_done = false;
    }

    public TupleDesc getTupleDesc() { //one field tuple
        // some code goes here
    	Type[] types = new Type[1];
    	types[0] = Type.INT_TYPE;
    	String[] names = new String[1];
    	names[0] = "insert_count";
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
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (m_done)
    		return null;
    	
    	int insert_count = 0;
    	try {
    		while (m_child.hasNext()) {
    			Tuple next = m_child.next();
    			insert_count++;
    			Database.getBufferPool().insertTuple(m_tid, m_tabId, next);
    		}
    	} catch(IOException e) {
    		System.err.println("Error inserting tuple");
    		System.exit(1);
    	}
    	
    	IntField intField = new IntField(insert_count);
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
