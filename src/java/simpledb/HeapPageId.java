package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {
	
	private int m_tableId;
	private int m_pgNum;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        // some code goes here
    	m_tableId = tableId;
    	m_pgNum = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        // some code goes here
        return m_tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int pageNumber() {
        // some code goes here
        return m_pgNum;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // some code goes here
    	int n = m_pgNum,
    		i = 0;
    	
    	while(n>0) {
    		n = n/10;
    		i++;
    	}
    	
    	return (int) (m_tableId*Math.pow(10, i) + m_pgNum);
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        // some code goes here
    	if(o==null) {
    		return false;
    	}
    	
    	HeapPageId hp;
    	try {
    		hp = (HeapPageId)o;
    	} catch(ClassCastException e) {
    		return false;
    	}
    	
    	if(m_pgNum==hp.pageNumber() && m_tableId==hp.getTableId()) {
    		return true;
    	} else {
    		return false;
    	}   	
        
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = pageNumber();

        return data;
    }

}
