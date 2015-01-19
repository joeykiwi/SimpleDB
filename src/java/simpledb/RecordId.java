package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private PageId m_pgId;
    private int m_tupNum;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // some code goes here
    	m_pgId = pid;
    	m_tupNum = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        // some code goes here
        return m_tupNum;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return m_pgId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
        RecordId rid = (RecordId)o;
        if(m_pgId.equals(rid.m_pgId) && m_tupNum==rid.m_tupNum) {
        	return true;
        } else {
        	return false;
        }
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
    	int n = m_tupNum,
        	i = 0;
        	
        while(n>0) {
        	n = n/10;
        	i++;
        }
        
    	return (int) (m_pgId.hashCode()*Math.pow(10, i)+m_tupNum);

    }

}
