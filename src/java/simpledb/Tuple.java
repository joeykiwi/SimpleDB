package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private ArrayList<Field> m_fields;
    private RecordId m_rid;
    private TupleDesc m_td;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
    	m_td = td;
    	m_rid = null;
    	m_fields = new ArrayList<Field>();
    	
    	for(int i=0; i<td.numFields(); i++) {
    		Field newField;
    		if(td.getFieldType(i)==Type.INT_TYPE) {
    			newField = new IntField(0);
    			m_fields.add(newField);
    		} else if(td.getFieldType(i)==Type.STRING_TYPE) {
    			newField = new StringField("", 0);
    			m_fields.add(newField);
    		} else { //error
    			System.err.println("invalid field type");
        		System.exit(1);
    		}
    		
    	}
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return m_td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return m_rid;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
    	m_rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
    	if(i<0 || i>=m_fields.size()) { //invalid index
    		System.err.println("Out of bound: field index set");
    		System.exit(1);
    	}
    	m_fields.set(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
    	if(i<0 || i>=m_fields.size()) {
    		return null;
    	}
        return m_fields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        // some code goes here
    	//ex: (5 6 9)    	
    	String result ="";
    	for(int i=0; i<m_fields.size(); i++) {
    		if(i==m_fields.size()-1) {
    			result += m_fields.get(i).toString();
    		} else {
    			result += m_fields.get(i).toString() + " ";
    		}
    		
    	}
	return result;
        
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
    	Iterator<Field> itr = m_fields.iterator();
        return itr;
    }
    
    /**
     * reset the TupleDesc of this tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
    	m_td = td;
    }
}
