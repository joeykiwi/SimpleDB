package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    private int m_gbIndex;
    private int m_agIndex;
    private String m_gbName;
    private Op m_op;
    private Type m_gbType;
    private HashMap<Object, Integer> m_data; //keep the calculation result for each group
    private HashMap<Object, Integer> m_count; //keep the number of tuples for each group to use to calculate average
    
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	m_gbIndex = gbfield;
    	m_agIndex = afield;
    	m_op = what;
    	m_gbType = gbfieldtype;
    	m_gbName = null;
    	if(m_op == Op.COUNT) {
    		m_count = new HashMap<Object, Integer>();
    	} else if(m_op == Op.AVG) {
    		m_count = new HashMap<Object, Integer>();
    		m_data = new HashMap<Object, Integer>();
    	} else {
    		m_data = new HashMap<Object, Integer>();
    		m_count = null;
    	}
    	
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	// some code goes here    	
    	//System.out.println("merging");    	
    	
    	//set and check gbname
    	if(m_gbIndex==Aggregator.NO_GROUPING) {
    		m_gbName = "no_grouping";
    		//do nothing
    	} else if(m_gbName==null ) {
    		m_gbName = tup.getTupleDesc().getFieldName(m_gbIndex);
    	} else if(m_gbName == tup.getTupleDesc().getFieldName(m_gbIndex) ) {
    		// do nothing
    	} else {
    		System.err.println("Inconsistent group field name");
    		System.exit(1);
    	}
    	
    	//check gbtype
    	if(m_gbIndex!=Aggregator.NO_GROUPING && m_gbType != tup.getTupleDesc().getFieldType(m_gbIndex)) {
    		System.err.println("Input tuple has inconsistent type for groupby field");
    		System.exit(1);
    	}
    	
    	IntField agif = (IntField) tup.getField(m_agIndex);
    	Integer newVal = new Integer(agif.getValue());  	
    	
    	//no grouping
    	if(m_gbIndex == Aggregator.NO_GROUPING) {
    		//System.out.println("no grouping calculate");
    		Integer i = new Integer(-1);
    		calculate(i, newVal);	    		
    		return;
    	}
    	
    	//grouping by INT or STRING
    	if(m_gbType == Type.INT_TYPE) {
    		IntField gbif = (IntField) tup.getField(m_gbIndex);
        	Integer gbVal = new Integer(gbif.getValue());
    		calculate(gbVal, newVal);    		
    	} else if(m_gbType == Type.STRING_TYPE){
    		StringField gbsf = (StringField)  tup.getField(m_gbIndex);
    		String gbVal = gbsf.getValue();
    		calculate(gbVal, newVal);
    	}
    	
    	return;
    }

    //calculate the new result to put into m_data for the corresponding group value
    public void calculate(Object gbVal, Integer newVal) {
    	
    	Integer oldVal;
    	
    	if(m_op==Op.COUNT)
    		oldVal = m_count.get(gbVal);
    	else
    		oldVal = m_data.get(gbVal);
    	
    	Integer resultVal = null;
    	int oldInt = 0;
    	int newInt = newVal.intValue();
    	
    	if(oldVal==null) { //it's the first tuple in the group
    		Integer newCount = new Integer(1);
    		if(m_op==Op.COUNT) {
    			m_count.put(gbVal, newCount);
    		} else if(m_op==Op.AVG) {
    			m_data.put(gbVal, newVal);
    			m_count.put(gbVal, newCount);
    		} else {
    			m_data.put(gbVal, newVal);
    		}
    		
    		return;
    	} else {
    		oldInt = oldVal.intValue();
    	}
    	
    	
    	switch(m_op) {
    		
	    	case SUM:
	    	{
	    		resultVal = new Integer(oldInt+newInt);
	    		m_data.put(gbVal,  resultVal);
	    		return;
	    	}
	    	
	    	case MIN:
	    	{
	    		resultVal = new Integer(oldInt<newInt? oldInt : newInt);
	    		m_data.put(gbVal,  resultVal);
	    		return;
	    	}
	    	
	    	case MAX:
	    	{
	    		resultVal = new Integer(oldInt>newInt? oldInt : newInt);
	    		m_data.put(gbVal,  resultVal);
	    		return;
	    	}
	    	case AVG:
	    	{
	    		Integer countObj = m_count.get(gbVal);
	    		resultVal = new Integer(oldInt+newInt);
	    		int count = countObj.intValue();
	    		m_count.put(gbVal, new Integer(count+1));
	    		m_data.put(gbVal, resultVal);
	    		return;
	    	}
	    	
	    	case COUNT:
	    	{
	    		resultVal = new Integer(oldInt+1);
	    		m_count.put(gbVal, resultVal);
	    		return;
	    	}
	    	
	    	default:
	    	{
	    		System.err.println("invalid aggregate operator");
	    		System.exit(1);
	    	}
    	
    	}
    }
    
    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	LinkedList<Tuple> tuples = new LinkedList<Tuple>();
    	TupleIterator itr = null; 	
    	TupleDesc td = null;
    	int result = -989;
    	
    	
    	if(m_gbIndex == Aggregator.NO_GROUPING) { //no grouping
    		Integer no_group = new Integer(-1);
    		Integer res;
    		if(m_op==Op.COUNT) {
    			res = m_count.get(no_group);
    		} else if(m_op==Op.AVG) {
    			Integer count = m_count.get(no_group);
    			Integer totalSum = m_data.get(no_group);
    			res = new Integer(totalSum.intValue()/count.intValue());
    		} else {
    			 res = m_data.get(no_group);
    		}
    		
    		if(res==null) {
    			throw new RuntimeException("no aggregate value for no grouping case" + no_group);
    			//return null;
    		}
    		result = res.intValue();
    		
    		//create arrays for tuple descriptor
    		Type[] typeArr = new Type[1];
    		typeArr[0] = Type.INT_TYPE;
    		String[] strArr = new String[1];
    		strArr[0] = m_op.toString();
    		
    		//create a tuple
    		TupleDesc newTd = new TupleDesc(typeArr, strArr);
    		Tuple newTup = new Tuple(newTd);
    		
    		//create a field for aggregate value
    		IntField newField = new IntField(result);
    		
    		//set field
    		newTup.setField(0, newField);
    		
    		//return the result (one tuple with a single agg val)
    		tuples.add(newTup);
    		itr = new TupleIterator(newTd, tuples);
    		return itr;
    	}
    	
    	HashMap<Object, Integer> toCheck = m_op==Op.COUNT? m_count: m_data;
    	
    	for(Map.Entry<Object, Integer> entry : toCheck.entrySet()) { //for each group
    		int agVal = entry.getValue().intValue();
    		if(m_gbType == Type.INT_TYPE) {
    			Integer gbKey = (Integer) entry.getKey();
    			
    			//create arrays
    			Type[] typeArr = new Type[2];
    			typeArr[0] = m_gbType;
    			typeArr[1] = Type.INT_TYPE;
    			String[] strArr = new String[2];
    			strArr[0] = m_gbName;
    			strArr[1] = m_op.toString();
    			
    			//create tuple
    			TupleDesc newTd = new TupleDesc(typeArr, strArr);
    			Tuple newTup = new Tuple(newTd);
    			
    			//create fields
    			IntField newField0 = new IntField(gbKey.intValue());
    			IntField newField1;
    			if(m_op==Op.AVG) {
    				int count = m_count.get(gbKey).intValue();
    				newField1 = new IntField(agVal/count); //average = total sum / count
    			} else {		
    				newField1 = new IntField(agVal);
    			}
    			
    			//set fields
    			newTup.setField(0, newField0);
    			newTup.setField(1, newField1);
    			
    			//result
    			tuples.add(newTup);
    			td = newTd;
    			
    		} else { //group by String Type
    			String gbKey = (String) entry.getKey();
    			
    			//create arrays
    			Type[] typeArr = new Type[2];
    			typeArr[0] = m_gbType;
    			typeArr[1] = Type.INT_TYPE;
    			String[] strArr = new String[2];
    			strArr[0] = m_gbName;
    			strArr[1] = m_op.toString();
    			
    			//create tuple
    			TupleDesc newTd = new TupleDesc(typeArr, strArr);
    			Tuple newTup = new Tuple(newTd);
    			
    			//create fields
    			StringField newField0 = new StringField(gbKey, gbKey.length());		
    			IntField newField1;
    			if(m_op==Op.AVG) {
    				int count = m_count.get(gbKey).intValue();
    				newField1 = new IntField(agVal/count); //average = total sum / count
    			} else {		
    				newField1 = new IntField(agVal);
    			}
    			
    			
    			//set fields
    			newTup.setField(0, newField0);
    			newTup.setField(1, newField1);
    			
    			//result
    			tuples.add(newTup);
    			td = newTd;
    		}
    	}
    	
		itr = new TupleIterator(td, tuples);
		return itr;
    	
    }

}
