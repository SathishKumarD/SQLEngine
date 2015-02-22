/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.statement.select.Limit;

/**
 * @author Sathish
 *
 */
public class LimitOperator implements Operator {

	private Limit limit;
	private long counter;
	private Operator source;

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */
	@Override
	public ArrayList<Tuple> readOneTuple() {
		// TODO Auto-generated method stub
		if (limit.getOffset() > counter) {
			counter++;
			return readOneTuple();
		}		
		if (limit.isLimitAll()){
			return this.source.readOneTuple();
		}
		else if (counter < limit.getRowCount()){
			counter++;
			return this.source.readOneTuple();
		}
		return null;
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
	
	public LimitOperator(Operator input, Limit limitObj){
		this.limit = limitObj;
		this.counter = 0;
		this.source = input;
	}

}
