package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int m_gbIndex;
    private int m_agIndex;
    private String m_gbName;
    private Op m_op;
    private Type m_gbType;
    private HashMap<Object, Integer> m_data;
    
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	if(what!=Op.COUNT) {
    		System.err.println("Invalid aggregate operator for string field");
    		System.exit(1);
    	}
    	m_gbIndex = gbfield;
    	m_agIndex = afield;
    	m_gbType = gbfieldtype;
    	m_op = what;
    	m_data = new HashMap<Object, Integer>();
    	m_gbName = null;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	
    	//set and check gbname
    	if(m_gbName==null) {
    		m_gbName = tup.getTupleDesc().getFieldName(m_gbIndex);
    	} else if(m_gbName == tup.getTupleDesc().getFieldName(m_gbIndex)) {
    		// do nothing
    	} else {
    		System.err.println("Inconsistent group field name");
    		System.exit(1);
    	}
    	
    	//check gbtype
    	if(m_gbType != tup.getTupleDesc().getFieldType(m_gbIndex)) {
    		System.err.println("Input tuple has inconsistent type for groupby field");
    		System.exit(1);
    	}
    	
    	//no grouping
    	if(m_gbIndex == Aggregator.NO_GROUPING) {
    		String s = "no_group";
    		Integer oldVal = m_data.get(s);
    		int oldCount = 0;
    		if(oldVal==null) { //it's the first tuple in the group
    			m_data.put(s, new Integer(1));
    			return;
    		}
    		oldCount = oldVal.intValue();
    		m_data.put(s, new Integer(oldCount+1));
    		return;
    	}
    	
    	//grouping by INT or STRING
    	if(m_gbType == Type.INT_TYPE) {
    		IntField gbif = (IntField) tup.getField(m_gbIndex);
        	Integer gbVal = new Integer(gbif.getValue());
        	Integer oldVal = m_data.get(gbVal);
        	int oldCount = 0;
        	if(oldVal==null) { //it's the first tuple in the group
        		m_data.put(gbVal,  new Integer(1));
        		return;
        	}
    		oldCount = oldVal.intValue();
    		m_data.put(gbVal, new Integer(oldCount+1));

    	} else if(m_gbType == Type.STRING_TYPE){
    		StringField gbsf = (StringField)  tup.getField(m_gbIndex);
    		String gbVal = gbsf.getValue();
    		Integer oldVal = m_data.get(gbVal);
    		int oldCount = 0;
    		if(oldVal==null) { //it's the first tuple in the group
    			m_data.put(gbVal, new Integer(1));
    			return;
    		}
    		oldCount = oldVal.intValue();
    		m_data.put(gbVal, new Integer(oldCount+1));
    		
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	LinkedList<Tuple> tuples = new LinkedList<Tuple>();
    	TupleIterator itr = null; 	
    	TupleDesc td = null;
    	
    	if(m_gbIndex == Aggregator.NO_GROUPING) { //no grouping
    		int result = m_data.get("no_group").intValue();
    		
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
    	
    	for(Map.Entry<Object, Integer> entry : m_data.entrySet()) { //for each group
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
    			IntField newField1 = new IntField(agVal);
    			
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
    			IntField newField1 = new IntField(agVal);
    			
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
