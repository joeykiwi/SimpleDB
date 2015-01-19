package simpledb;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	
	private File m_file;
	private TupleDesc m_td;
	private FileChannel m_fc;
	private LinkedList<PageId> m_freePgIds;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	m_file = f;
    	m_td = td;
    	m_freePgIds = new LinkedList<PageId>();
        try {
            RandomAccessFile raf = new RandomAccessFile(f, "rw");
            m_fc = raf.getChannel();                       
        } catch(IOException e) {
            System.err.println("error creating a channel");
            System.exit(1);
        }
        
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return m_file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
    	return m_file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return m_td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here
    	int pgNum = pid.pageNumber();
        int offset = BufferPool.PAGE_SIZE * pgNum;
        
        if(pgNum>=numPages()) { //page number is out of range
    		throw new IllegalArgumentException();
    	}
        
        try {
        	ByteBuffer buffer = ByteBuffer.allocate(BufferPool.PAGE_SIZE);
            m_fc.read(buffer, offset);
            HeapPageId hpid = (HeapPageId) pid;
            return new HeapPage(hpid, buffer.array());
        } catch (IOException e) {
            System.err.println("error reading a page");
            System.exit(1);
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    	int pgNum = page.getId().pageNumber();
    	int offset = BufferPool.PAGE_SIZE * pgNum;
    	
    	ByteBuffer buffer = ByteBuffer.wrap(page.getPageData());
    	m_fc.write(buffer, offset);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
    	int res = 0;
        try {
            res = (int) Math.ceil(this.m_fc.size() / BufferPool.PAGE_SIZE);            
        } catch (IOException e) {
            System.err.println("error counting the number of pages");
            System.exit(1);
        }
        
        return res;
    }
    
    public HeapPageId findNextFreePageId(TransactionId tid)
    	throws DbException, IOException, TransactionAbortedException { //grab a next free page using buffer pool
    	
    	for(PageId pid: m_freePgIds) { //look for free page id remembered by the heap file object
    		return (HeapPageId)pid;
    	}
    	
    	//the heap file object does not remember any free page ids
    	for(int i=0; i<numPages(); i++) { //for each page in the file
    		PageId tempPid = new HeapPageId(getId(), i);
    		HeapPage tempPg = (HeapPage) Database.getBufferPool().getPage(tid, tempPid, Permissions.READ_WRITE);
    		if(tempPg.getNumEmptySlots()>0) {
    			//System.out.println("found free pg :"+tempPg.getId().pageNumber()+";"+i+" - " + tempPg.getNumEmptySlots()+"/"+tempPg.getNumSlots() );
    			PageId targetPid = tempPg.getId();
    			return tempPg.getId();
    		}
    	}
    	
    	return null;
    }
    
    public HeapPageId allocateNewPage() throws IOException { //allocate a new page and return the id
    	HeapPageId newPid = new HeapPageId(getId(), numPages());
    	try {
    		HeapPage newPg = new HeapPage(newPid, HeapPage.createEmptyPageData());
    		writePage(newPg);
    		return newPg.getId();
    	} catch(IOException i) {
    		System.err.println("error allocating a heap page");
    		System.exit(1);
    	}
    	return null;
    }
    
    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	
    	/*
    	psudo code:
    		search for the next free page id
    		if success
    			accces the free page using buffer pool
    		else
    			allocate a new page to the heapfile
    			accces the new page using buffer pool
    		add the tuple
    		mark the page to dirty
    		
    	*/
    	
    	HeapPageId nextFreePgId = findNextFreePageId(tid);
    	if(nextFreePgId == null) {
    		nextFreePgId = allocateNewPage();
    	}
    	
    	HeapPage nextFreePg = (HeapPage)Database.getBufferPool().getPage(tid, nextFreePgId, Permissions.READ_WRITE);
    	nextFreePg.insertTuple(t);
    	if(nextFreePg.getNumEmptySlots() == 0) {
    		m_freePgIds.remove(nextFreePgId);
    	}
    	nextFreePg.markDirty(true, tid);
    	//the page is modified, but the modification is only stored in buffer pool
    	
    	ArrayList<Page> result = new ArrayList<Page>();
    	result.add(nextFreePg);
    	return result;
    	
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	PageId targetPid = t.getRecordId().getPageId();
    	HeapPage targetPg = (HeapPage) Database.getBufferPool().getPage(tid, targetPid, Permissions.READ_WRITE);
    	targetPg.deleteTuple(t);
    	targetPg.markDirty(true, tid);
    	
    	m_freePgIds.add(targetPid); //add this page to the free page list
    								//since after deletion, there will be at least one open slot
    	
    	ArrayList<Page> result = new ArrayList<Page>();
    	result.add(targetPg);
    	
    	return result;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
    	DbFileIterator hfitr = new HeapFileIterator(tid, this);
        return hfitr;
    }

}

