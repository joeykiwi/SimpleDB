package simpledb;

import java.util.*;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator m_child;
    private DbIterator m_itr;
    private int m_agIndex;
    private int m_gbIndex;
    private Aggregator.Op m_op;
    private Aggregator m_agg;
    
    
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
    	// some code goes here
    	m_child = child;
    	m_agIndex = afield;
    	m_gbIndex = gfield;
    	m_op = aop;
    	if(child.getTupleDesc().getFieldType(afield)==Type.INT_TYPE) {
    		if(gfield==Aggregator.NO_GROUPING) {
    			m_agg = new IntegerAggregator(Aggregator.NO_GROUPING, null, afield, aop);
    		} else {
    			m_agg = new IntegerAggregator(gfield, child.getTupleDesc().getFieldType(m_gbIndex), afield, aop);
    		}
    	} else {
    		if(gfield==Aggregator.NO_GROUPING) {
    			m_agg = new StringAggregator(Aggregator.NO_GROUPING, null, afield, aop);
    		} else {
    			m_agg = new StringAggregator(gfield, child.getTupleDesc().getFieldType(m_gbIndex), afield, aop);
    		}
    	}
    	
    	generateIterator();
	    
    	
    }
    
    public void generateIterator() {
    	//merge all tuples in the child into the aggregator
    	
    	int i=0;
    	int j=0;
    	
    	try {
    		m_child.open();
		    while(m_child.hasNext()) { j++;
		    	i=1;
		    	Tuple next = m_child.next();
		    	//System.out.println(next+" "+next.getTupleDesc())
		    	i=2;
		    	m_agg.mergeTupleIntoGroup(next);
		    	i=3;
		    }
    	} catch(DbException d) {
    		System.out.println("db: failure to generate aggregate iterator "+j+" loops "+i);
    		m_itr = null;
    		//System.exit(1);
    	} catch(TransactionAbortedException t) {
    		System.out.println("tr: failure to generate aggregate iterator "+j+" loops "+i);
    		m_itr = null;
    		//System.exit(1);
    	} catch(NoSuchElementException n) {
    		System.out.println("noElem: failure to generate aggregate iterator "+j+" loops "+i);
    		m_itr = null;
    		//System.exit(1);
    	}
    	
	    //set iterator for the aggregation
	    m_itr = m_agg.iterator();
	    m_child.close();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
    	// some code goes here
    	return m_gbIndex;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
    	// some code goes here
    	return m_child.getTupleDesc().getFieldName(m_gbIndex);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
		// some code goes here
		return m_agIndex;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
		// some code goes here
		return m_child.getTupleDesc().getFieldName(m_agIndex);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
		// some code goes here
		return m_op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
    	// some code goes here
    	if(m_itr!=null) {
    		super.open();
    		m_itr.open();
    	}
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	//some code goes here
    	if(m_itr.hasNext())
    		return m_itr.next();
    	else
    		return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	//some code goes here
    	if(m_itr!=null) {
    		m_itr.rewind();
    	}
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
		// some code goes here
		return m_itr.getTupleDesc();
    }

    public void close() {
    	// some code goes here
    	if(m_itr!=null) {
    		super.close();
    		m_itr.close();
    	}
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
