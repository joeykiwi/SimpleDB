package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    private int m_fieldNumber1;
    private int m_fieldNumber2;
    private Predicate.Op m_operator;
    
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        // some code goes here
    	m_fieldNumber1 = field1;
    	m_fieldNumber2 = field2;
    	m_operator = op;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here
    	Field f1 = t1.getField(m_fieldNumber1);
    	Field f2 = t2.getField(m_fieldNumber2);    	
        return f1.compare(m_operator, f2);
    }
    
    public int getField1()
    {
        // some code goes here
        return m_fieldNumber1;
    }
    
    public int getField2()
    {
        // some code goes here
        return m_fieldNumber2;
    }
    
    public Predicate.Op getOperator()
    {
        // some code goes here
        return m_operator;
    }
}
