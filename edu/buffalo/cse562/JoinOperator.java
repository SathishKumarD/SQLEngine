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
public class JoinOperator implements Operator {

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */
	
	//TODO: Create setters and getters
	private Operator left;
	private Operator right;
	
	@Override
	public ArrayList<Tuple> readOneTuple() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#reset()
	 */
	@Override
	public void reset() {
		// TODO Auto-generated method stub
	}

	public JoinOperator(Operator left, Operator right){
		this.left = left;
		this.right = right;
	}
	
	public String toString(){
		StringBuilder b = new StringBuilder("JOIN WITH \n");
		b.append('\t' +this.right.toString() + '\n');
		return b.toString();
	}
	
	public Operator peekNextOp(){
		return this.left;
	}

	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		// TODO Auto-generated method stub
		return null;
	}
}
