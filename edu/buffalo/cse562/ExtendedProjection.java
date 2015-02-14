/**
 * 
 */
package edu.buffalo.cse562;

import net.sf.jsqlparser.statement.select.SelectExpressionItem;

/**
 * @author Sathish
 *
 */
public class ExtendedProjection implements Operator {

	Operator source;
	SelectExpressionItem selexp;
	public ExtendedProjection(Operator current, SelectExpressionItem s) {
		// TODO Auto-generated constructor stub
		this.source = current;
		this.selexp = s;
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */
	@Override
	public Datum[] readOneTuple() {
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
	
	public String toString(){
		return "SELECT " + this.selexp.toString();
	}
	
	public Operator peekNextOp(){
		return this.source;
	}
}
