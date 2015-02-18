/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.statement.select.SelectExpressionItem;

/**
 * @author Sathish
 *
 */
public class ExtendedProjection implements Operator {

	Operator input;
	SelectExpressionItem selexp;
	private  HashMap<String, ColumnDetail> inputSchema = null;
	
	public ExtendedProjection(Operator input, SelectExpressionItem s) {
		// TODO Auto-generated constructor stub
		this.input = input;
		this.selexp = s;
		this.inputSchema = input.getOutputTupleSchema();
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */
	@Override
 	public ArrayList<Tuple> readOneTuple() {
		// TODO Auto-generated method stub
		
		return input.readOneTuple();
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#reset()
	 */
	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}
	
	public String toString(){
		return "SELECT " + this.selexp.toString();
	}
	
	public Operator peekNextOp(){
		return this.input;
	}

	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		// TODO Here input schema gets changed. columns will be removed.
		return null;
	}
}
