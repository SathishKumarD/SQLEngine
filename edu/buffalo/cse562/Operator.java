/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author Sathish
 *
 */
public interface Operator {
	
	/**
	 * returns one tuple at a time
	 * @return
	 */
	public ArrayList<Tuple> readOneTuple();
	
	/**
	 * resets the iterator to the initial item
	 */
	public void reset();
	
	/**
	 * Returns its child operator
	 * @return
	 */
	public Operator peekNextOp();
	
	/**
	 * Returns the output tuple schema the implementer may produce
	 * @return
	 */
	public HashMap<String,ColumnDetail> getOutputTupleSchema();
}
