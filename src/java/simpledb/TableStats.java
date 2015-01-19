package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {
	
	private class HistogramBundle {
		public int min;
		public int max;
		public boolean touched;
		public Object histogram;
		
		public HistogramBundle() {
			min = 0;
			max = 0;
			touched = false;
			histogram = null;
		}
		
		public boolean isTouched() {
			return touched;
		}
		
		public void setTouched() {
			touched = true;
		}
		
	}
	
	
	private int m_cost; //cost per page
	private int m_numFields;
    private HeapFile m_file; //a file corresponding to the relation
    private HistogramBundle[] m_histograms; // there is one histogram per field in the relation	

    
    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    	m_file = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
    	m_cost = ioCostPerPage;
    	m_numFields = m_file.getTupleDesc().numFields();
    	m_histograms = new HistogramBundle[m_numFields];
    	for(int i=0; i<m_histograms.length; i++) {
    		m_histograms[i] = new HistogramBundle();
    	}
    	initHistogramBundles();
    }
    
    public void initHistogramBundles() {
    	try {
    		TransactionId currTr = new TransactionId();
        	DbFileIterator itr = m_file.iterator(currTr);
        	itr.open();
        	
    		initMinMax(itr);
    		initHistograms();
    		fillHistograms(itr);
    	} catch(DbException be) {
    		System.err.println("TableStats: initHistogramBundles - DbException");
            System.exit(1);
    	} catch(TransactionAbortedException te) {
    		System.err.println("TableStats: initHistogramBundles - TAException");
            System.exit(1);
    	}
    }
    
    public void initMinMax(DbFileIterator itr) throws DbException, TransactionAbortedException {
    	itr.rewind();
    	while(itr.hasNext()) { //for each tuple in the relation
    		Tuple currTup = itr.next();
    		
    		for(int i=0; i<m_numFields; i++) { //for each field in the tuple
    			Field currField = currTup.getField(i);
    			HistogramBundle currHB = m_histograms[i];
    			
    			if(currField.getType() == Type.INT_TYPE) {
    				int currIntVal = ((IntField)currField).getValue();
    				if(currHB.isTouched()) {
    					if(currIntVal>currHB.max) {
    						currHB.max = currIntVal;
    					} else if(currIntVal<currHB.min) {
    						currHB.min = currIntVal;
    					}
    				} else { //this tuple is the first one to be inserted into the histogram
    					currHB.max = currIntVal;
    					currHB.min = currIntVal;
    					currHB.setTouched();
    				}
    			} else if(currField.getType() == Type.STRING_TYPE) {
    				//do nothing
    			} else {
    				System.err.println("TableStats: initMinMax");
                    System.exit(1);
    			}
    		}
    	}
    }
    
    
    
    //assume the min and max values of each HistogramBundle are found 
    public void initHistograms() {
    	TupleDesc tableDesc = m_file.getTupleDesc();
    	
    	for(int i=0; i<m_numFields; i++) {
    		Type currFieldType = tableDesc.getFieldType(i);
    		if(currFieldType==Type.INT_TYPE) {
        		int currMin = m_histograms[i].min;
        		int currMax = m_histograms[i].max;
    			m_histograms[i].histogram = new IntHistogram(NUM_HIST_BINS, currMin, currMax);
    		} else if(currFieldType==Type.STRING_TYPE) {
    			m_histograms[i].histogram = new StringHistogram(NUM_HIST_BINS);
    		} else {
    			System.err.println("TableStats: initHistograms - invalid type");
                System.exit(1);
    		}
    	}
    }
    
    
    
    public void fillHistograms(DbFileIterator itr) throws DbException, TransactionAbortedException {
    	itr.rewind();
    	while(itr.hasNext()) { //for each tuple
    		Tuple currTup = itr.next();
    		
    		for(int i=0; i<m_numFields; i++) { //for each field
    			Field currField = currTup.getField(i);
    			HistogramBundle currHB = m_histograms[i];
    			
    			if(currField.getType() == Type.INT_TYPE) {
    				int currIntVal = ((IntField)currField).getValue();
    				((IntHistogram)currHB.histogram).addValue(currIntVal); //add the value to the corresponding histogram
    			} else if(currField.getType() == Type.STRING_TYPE) {
    				String currStrVal = ((StringField)currField).getValue();
    				((StringHistogram)currHB.histogram).addValue(currStrVal);
    			} else {
    				System.err.println("TableStats: fillHistograms");
                    System.exit(1);
    			}
    		}
    	}
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return m_file.numPages() * m_cost;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
    	return (int) Math.floor(selectivityFactor * totalTuples());
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
    	/*
    	HistogramBundle histBundle = m_histograms[field];    	
    	double result = 0;
    	
    	if(m_file.getTupleDesc().getFieldType(field) == Type.INT_TYPE) {
    		double totalSelectivity = 0;
    		
    		for(int v=histBundle.min; v<=histBundle.max; v++) { //for every possible value
    			totalSelectivity += ((IntHistogram)(histBundle.histogram)).estimateSelectivity(op, v);	
    		}
    		result = totalSelectivity / (double)(histBundle.max-histBundle.max+1);
    		
    	} else {
    		result = ((StringHistogram)(histBundle.histogram)).avgSelectivity();
    	}
    	*/
    	
    	HistogramBundle histBundle = m_histograms[field];
    	
    	switch (op) {
			case NOT_EQUALS:
			{
				int range = histBundle.max-histBundle.min;
				return 1 - 1/(double)range ;
				
			}
			case EQUALS:
			{
				int range = histBundle.max-histBundle.min;
				return 1/(double)range ;
			}
			case GREATER_THAN_OR_EQ:
			case GREATER_THAN:
			case LESS_THAN_OR_EQ:					
			case LESS_THAN:
			case LIKE:
			{
				return 0.3;
			}
    	}
        
    	//error
    	return -1;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
    	double result = 0;
    	HistogramBundle histBundle = m_histograms[field];
    	
    	if(m_file.getTupleDesc().getFieldType(field) == Type.INT_TYPE) {
    		int intVal = ((IntField) constant).getValue();
    		result = ((IntHistogram)(histBundle.histogram)).estimateSelectivity(op, intVal);
    		
    	} else {
    		String strVal = ((StringField) constant).getValue();
    		result = ((StringHistogram)(histBundle.histogram)).estimateSelectivity(op, strVal);
    	}
    	
        return result;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
    	int result = 0;
    	
    	//choose field 0 arbitrarily to count the total number of tuples in the relation
    	if(m_file.getTupleDesc().getFieldType(0) == Type.INT_TYPE) { //field 0 is INT_TYPE
    		result = ((IntHistogram)(m_histograms[0].histogram)).totalCount();
    	} else { 
    		result = ((StringHistogram)(m_histograms[0].histogram)).totalCount();
    	}
    	
    	return result;
    }
    
    public String toString() { //for debugging
    	String result ="";
    	return Database.getCatalog().getTableName(m_file.getId());
        
    }

}
