package simpledb;

import java.util.*;

public class HeapFileIterator implements DbFileIterator {
    private TransactionId m_tid;
    private HeapFile m_file;
    private int m_numPages; //total # of pages in the heap file
    
    private int m_currPgNum; //current page number
    private int m_currTupNum;
    private HeapPageIterator m_hpgItr;
    private HeapPage m_currPg;

    public HeapFileIterator(TransactionId tid, HeapFile file) {
        m_tid = tid;
        m_file = file;
        m_currPgNum = 0;
        m_currTupNum = 0;
        m_currPg = null;
        m_numPages = m_file.numPages();
        m_hpgItr = null;        
    }

    public void open()
        throws DbException, TransactionAbortedException {
    	if(m_hpgItr!=null) {
    		throw new DbException("opened twice");
    	}
    	PageId firstPagePid = new HeapPageId(m_file.getId(), 0);
        m_currPg = (HeapPage)Database.getBufferPool().getPage(m_tid, firstPagePid, Permissions.READ_ONLY);
        m_hpgItr = new HeapPageIterator(m_currPg);
        m_currPgNum = 0;
    }

    
    public boolean updateNextPage() 
    	throws DbException, TransactionAbortedException { //returns false if there is no more page
    	//update the current page to the next page and also update the iterator to iterate the next page
    	
    	PageId nextPageId = new HeapPageId(m_file.getId(), ++m_currPgNum);
    	try {
    		m_currPg = (HeapPage)Database.getBufferPool().getPage(m_tid, nextPageId, Permissions.READ_ONLY);
    		//m_currPg = (HeapPage) m_file.readPage(nextPageId);
    	} catch(IllegalArgumentException e) {
    		return false; //the reqcuested page does not exist
    	}
    	m_hpgItr = (HeapPageIterator) m_currPg.iterator();
    	return true;
    }
    
    public boolean hasNext()
        throws DbException, TransactionAbortedException {
    	
        if(m_hpgItr == null){
        	return false;
        }
        if(m_hpgItr.hasNext()) {
        	return true;
        }        
        
        HeapPageId tempPid;
        HeapPage tempPg;
        HeapPageIterator tempHitr;
        int tempPgNum = m_currPgNum;
        
        // look for other pages
        while(++tempPgNum < m_numPages) {
        	tempPid = new HeapPageId(m_file.getId(), tempPgNum);
        	try {
        		tempPg = (HeapPage) Database.getBufferPool().getPage(m_tid, tempPid, Permissions.READ_ONLY);
        	//m_currPg = (HeapPage) Database.getBufferPool().getPage(m_tid, pid, Permissions.READ_ONLY);
        	} catch(IllegalArgumentException e) {
        		return false; // no more page
        	}
        	tempHitr = (HeapPageIterator) tempPg.iterator();
        	if (tempHitr.hasNext()) {
        		return true;
        	}
        }
        
        return false;
        
    }

    public Tuple next()
        throws DbException, TransactionAbortedException, NoSuchElementException {
    	if(m_hpgItr==null) {
        	throw new NoSuchElementException("Tuple iterator not opened");
        }
    	
        if(m_hpgItr.hasNext()) {
        	m_currTupNum++;
        	return m_hpgItr.next();
        }
        
        // look for other pages
        while(m_currPgNum < m_numPages) {
        	if(!updateNextPage()) {  
        		close();
        		return null;
        	}
        	if (m_hpgItr.hasNext()) {
        		m_currTupNum++;
        		return m_hpgItr.next();
        	}
        }
        
        return null;	
    }
    
    public int getPageNum() {
    	return m_currPgNum;
    }
    
    public int getTupleNum() {
    	return m_currTupNum;
    }
    
    public int getNumTuples() {
    	return m_currPg.getNumSlots()-m_currPg.getNumEmptySlots();
    }
    
    public void rewind()
        throws DbException, TransactionAbortedException {
        close();
        open();
    }

    public void close() {
    	if(m_hpgItr==null) {
    		return;
    	}
        m_currPgNum = 0;
        m_hpgItr = null;
        m_currPg = null;   	
    }

}