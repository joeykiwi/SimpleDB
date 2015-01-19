package simpledb;

import java.util.*;


public class HeapPageIterator implements Iterator<Tuple> { //up-to-date with the page
    private HeapPage m_page;
    private int m_numTuples;
    private int m_currentTuple;
        
    // Assumes pages cannot be modified while iterating over them
    // Iterates over only valid tuples
    public HeapPageIterator(HeapPage page) {
        m_page = page;
        m_currentTuple = 0;
        if(page!=null) {
        	m_numTuples = m_page.getNumSlots();
        } else {
        	m_numTuples = 0;
        }
    }
        
    public boolean hasNext() {
    	if(m_currentTuple>=m_numTuples) { //iterator passsed the last element
    		return false;
    	}
    	
    	for(int i=m_currentTuple; i<m_numTuples; i++) { //check iteratively for the next valid tuple
    													//this is necessary because the page can be modified 
    													//after the iterator is created
    		if(m_page.isSlotUsed(i)) {
    			return true;
    		}
    	}
    	
    	
    	//nothing valud tuple left
        return false;
    }
        
    public Tuple next() { // assuming the tuple are not packed
    	if(m_currentTuple>=m_numTuples) { //no more tuple
    		return null;
    	} else if(m_page.isSlotUsed(m_currentTuple)) { //the current tuple is valid
    		return m_page.getTuple(m_currentTuple++);
    	} else { //the current tuple not valid
    		m_currentTuple++;
    		return next(); //recursively find the next valid tuple from the page
    	}
        
    }
        
    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Cannot remove on HeapPageIterator");
    }
}
