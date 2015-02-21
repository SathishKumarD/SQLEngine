/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Sathish
 *
 */
public class LimitOperator implements Operator {

	private int limit;
	private int counter;
	private Operator source;

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */
	@Override
	public ArrayList<Tuple> readOneTuple() {
		// TODO Auto-generated method stub
		if (counter <= limit){
			counter++;
			return this.source.readOneTuple();
		}
		else{
			return null;
		}
	}
	

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#reset()
	 */
	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}


	@Override
	public Operator peekNextOp() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public LimitOperator(Operator input, int limit){
		this.limit = limit;
		this.counter = 0;
		this.source = input;
	}

}
