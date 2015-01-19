package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
	
	private int m_numBuckets;
	private int m_min;
	private int m_max;
	private int m_range;
	private double m_width;
	private int[] m_buckets;
	
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	m_min = min;
    	m_max = max;
    	m_numBuckets = buckets;
    	m_range = m_max - m_min;
    	m_width = Math.ceil((double)m_range / (double)m_numBuckets);
    	m_buckets = new int[m_numBuckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    
    public int getIndex(int v) {
    	if(v<m_min)
    		return -1;
    	if(v>m_max)
    		return -2;
    	if ((v == m_max) && (v % m_width == 0)) {
    		v--;
    	}
    	
    	int offset = v - m_min;
    	int index = offset / (int) m_width;
    	
    	if(index>=m_numBuckets)
    		index = m_numBuckets-1;
    	else if(index<=0)
    		index = 0;
    	
    	return index;
    }
    
    public void addValue(int v) {
    	// some code goes here
    	if(v<m_min || v>m_max)
    		return;
    	int index = getIndex(v);
    	m_buckets[index]++;
    }

    public int totalCount() {
    	int result = 0;
    	for(int i=0; i<m_buckets.length; i++) {
    		result += m_buckets[i];
    	}
    	return result;
    }
    
    public int greaterThanCount(int start_index) { //start_index is not inclusive
    	int result = 0;
    	for(int i=start_index+1; i<m_buckets.length; i++) {
    		result += m_buckets[i];
    	}
    	return result;
    }
    
    public int lessThanCount(int end_index) { //end_index is not inclusive
    	int result = 0;
    	for(int i=0; i<end_index; i++) {
    		result += m_buckets[i];
    	}
    	return result;
    }
    
    //returns the right end point of the bucket of index=bucket+index
    public double getRightEndPoint(int bucket_index) {
    	return (bucket_index+1)*m_width + (double)m_min;
    }

    public double getLeftEndPoint(int bucket_index) {
    	return (bucket_index)*m_width + (double)m_min;
    }
    
    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
    	int ntups = totalCount();
    	int targetIndex = getIndex(v);
    	int heightOfTargetBucket = 0;
    	if(targetIndex >= 0)
    		heightOfTargetBucket = m_buckets[targetIndex];
    	double result = -1;
    	
    	switch (op) {
		case EQUALS:
			if(targetIndex==-1) { //v smaller than min
				result = 0.0;
			} else if(targetIndex==-2) { //v bigger than max
				result = 0.0;
			} else {
				result = ((double)heightOfTargetBucket / (double)m_width) / (double)ntups;
			}
			break;
			
		case GREATER_THAN:
			if(targetIndex==-1) { //v smaller than min
				result = 1.0;
			} else if(targetIndex==-2) { //v bigger than max
				result = 0.0;
			} else {
				double b_rest = (double)greaterThanCount(targetIndex) / (double)ntups;
				//double b_f = (double)m_buckets[targetIndex] / (double)ntups;
				//double b_part = (getRightEndPoint(targetIndex) - (double)v) / (double)m_width;
				result = b_rest; //does not include the targetBucket
			}
			break;
			
		case GREATER_THAN_OR_EQ:
			if(targetIndex==-1) { //v smaller than min
				result = 1.0;
			} else if(targetIndex==-2) { //v bigger than max
				result = 0.0;
			} else {
				double b_rest2 = (double)greaterThanCount(targetIndex) / (double)ntups;
				double b_f2 = (double)m_buckets[targetIndex] / (double)ntups;
				double b_part2 = (getRightEndPoint(targetIndex) - (double)v) / (double)m_width;
				//double b_equals2 = ((double)heightOfTargetBucket / (double)m_width) / (double)ntups;
				result = b_f2 + b_rest2; //includes the targetBucket
			}
			break;
			
		case LESS_THAN:
			if(targetIndex==-1) { //v smaller than min
				result = 0.0;
			} else if(targetIndex==-2) { //v bigger than max
				result = 1.0;
			} else {
				double b_rest3 = (double)lessThanCount(targetIndex) / (double)ntups;
				double b_f3 = (double)m_buckets[targetIndex] / (double)ntups;
				double b_part3 = ((double)v - getLeftEndPoint(targetIndex)) / (double)m_width;
				//result = b_f3 * b_part3 + b_rest3;
				result = b_rest3;
			}
			break;
			
		case LESS_THAN_OR_EQ:
			if(targetIndex==-1) { //v smaller than min
				result = 0.0;
			} else if(targetIndex==-2) { //v bigger than max
				result = 1.0;
			} else {
				double b_rest4 = (double)lessThanCount(targetIndex) / (double)ntups;
				double b_f4 = (double)m_buckets[targetIndex] / (double)ntups;
				double b_part4 = ((double)v - getLeftEndPoint(targetIndex)) / (double)m_width;
				double b_equals4 = ((double)heightOfTargetBucket / (double)m_width) / (double)ntups;
				//result = b_f4 * b_part4 + b_rest4 + b_equals4;
				result = b_f4 + b_rest4;
			}
			break;
			
		case LIKE:
			break;
		case NOT_EQUALS:
			if(targetIndex==-1) { //v smaller than min
				result = 1.0;
			} else if(targetIndex==-2) { //v bigger than max
				result = 1.0;
			} else {
				result = 1.0 - (((double)heightOfTargetBucket / (double)m_width) / (double)ntups);
			}
			break;
			
		default:
			break;
    	}
    	
    	if(result < 0 || result > 1) {
    		//System.err.println("IntHistogram: invalid selectivity "+result);
            //System.exit(1);
    	}
    	
    	if(result < 0) {
    		result = 0.0;
    	} else if(result > 1) {
    		result = 1.0;
    	}
    	
        return result;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }
}
