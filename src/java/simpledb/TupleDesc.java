package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

	private ArrayList<TDItem> m_items;
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
    	Iterator<TDItem> itr = m_items.iterator();
        return itr;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
    	m_items = new ArrayList<TDItem>();
    	for(int i=0; i<typeAr.length; i++) {
    		TDItem newItem = new TDItem(typeAr[i], fieldAr[i]);
    		m_items.add(newItem);
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	m_items = new ArrayList<TDItem>();
    	for(int i=0; i<typeAr.length; i++) {
    		TDItem newItem = new TDItem(typeAr[i], "");
    		m_items.add(newItem);
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return m_items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
    	if(i<0 || i>=m_items.size()) {
    		throw new NoSuchElementException();
    	}
        return m_items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	// some code goes here
    	if(i<0 || i>=m_items.size()) {
    		throw new NoSuchElementException("invalid index: "+i);
    	}
        return m_items.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
    	for(int i=0; i<m_items.size(); i++) {
    		if(m_items.get(i).fieldName.equals(name)) {
    			return i;
    		}
    	}
    	
    	throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    	int totalSize = 0;
    	for(int i=0; i<m_items.size(); i++){
    		totalSize += m_items.get(i).fieldType.getLen();
    	}
        return totalSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
    	int totalNumFields = td1.numFields()+td2.numFields(),
    		td1NumFields = td1.numFields();
    	Type[] resTypeArr = new Type[totalNumFields];
    	String[] resNameArr = new String[totalNumFields];
    	
    	for(int i=0; i<td1.numFields(); i++) {
    		resTypeArr[i] = td1.getFieldType(i);
    		resNameArr[i] = td1.getFieldName(i);
    	}
    		
    	for(int j=0; j<td2.numFields(); j++) {
    		resTypeArr[j+td1NumFields] = td2.getFieldType(j);
    		resNameArr[j+td1NumFields] = td2.getFieldName(j);
    	}
    	
    	TupleDesc newTD = new TupleDesc(resTypeArr, resNameArr);
    	
        return newTD;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
    	if(o==null) {
    		return false;
    	}
    	
    	TupleDesc inputTD;
    	
    	try {
    		inputTD = (TupleDesc)o;
    	} catch (ClassCastException e) { //o isn't a tupledesc
    		return false;
    	}
    	
    	if(this.numFields()!=inputTD.numFields()) {
    		return false;
    	}
    	
    	for(int i=0; i<this.numFields(); i++) {
    		if(this.getFieldType(i)!=inputTD.getFieldType(i)) {
    			return false;
    		}
    	}
    	
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
    	int res = 0;
    	for(int i=0; i<m_items.size(); i++) { 		
    		res += m_items.get(i).fieldType==Type.INT_TYPE? m_items.get(i).fieldName.hashCode(): m_items.get(i).fieldName.hashCode()+3; 
    	}
    	return res;
        //throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
    	String res = "";
    	
    	for(int i=0; i< m_items.size(); i++) {
    		if(i==m_items.size()-1) {
    			res += m_items.get(i).toString();
    		} else {
    			res += m_items.get(i).toString() + ", ";
    		}
    	}
    	
        return res;
    }
}
