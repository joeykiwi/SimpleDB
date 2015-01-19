package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    private TransactionId m_trId;
    private int m_tabId; //table id
    private String m_tabAlias;
    private DbFile m_file;
    private DbFileIterator m_itr;
    
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
    	m_trId = tid;
    	m_tabId = tableid;
    	m_tabAlias = tableAlias;
    	m_file = Database.getCatalog().getDatabaseFile(tableid);
    	m_itr = null;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(m_tabId);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
        // some code goes here
        return m_tabAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
    	m_tabId = tableid;
    	m_tabAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	HeapFile hf;
    	try {
    		hf = (HeapFile) m_file;
    	} catch(ClassCastException e) {
    		throw new DbException("Not a heap file");
    	}
    	m_itr = new HeapFileIterator(m_trId, hf);
    	m_itr.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc fileDesc = m_file.getTupleDesc();
        int descLength = fileDesc.numFields();
        String[] names = new String[descLength];
        Type[] types = new Type[descLength];        
        for (int i = 0; i < descLength; i++) {
            types[i] = fileDesc.getFieldType(i);
            names[i] = m_tabAlias + "." + fileDesc.getFieldName(i); //prefix with table alias
        }
        return new TupleDesc(types, names);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return m_itr.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        return m_itr.next();
    }

    public void close() {
        // some code goes here
    	m_itr.close();
    }
    
    public int getPageNum() {
    	HeapFileIterator hitr;
    	try {
    		hitr = (HeapFileIterator)m_itr;
    	} catch(ClassCastException e) {
    		return -1;
    	}
    	return hitr.getPageNum();
    }
    
    public int getTupleNum() {
    	HeapFileIterator hitr;
    	try {
    		hitr = (HeapFileIterator)m_itr;
    	} catch(ClassCastException e) {
    		return -1;
    	}
    	return hitr.getTupleNum();
    }

    public int getNumTuples() {    	
    	HeapFileIterator hitr;
    	try {
    		hitr = (HeapFileIterator)m_itr;
    	} catch(ClassCastException e) {
    		return -1;
    	}
    	return hitr.getTupleNum();
    }
    
    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	m_itr.rewind();
    }
    
    public DbFile getFile() {
    	return m_file;
    }
}
