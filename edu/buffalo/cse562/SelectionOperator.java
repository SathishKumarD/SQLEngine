/**
 * 
 */
package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.Expression;

/**
 * @author Sathish
 *
 */
public class SelectionOperator implements Operator {

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */
	
	Operator source;
	Expression exp;
	
	public SelectionOperator(Operator input, Expression exp){
		this.source = input;
		this.exp = exp;
	}
	
	public Datum[] readOneTuple() {
		// TODO Auto-generated method stub
		
		
		System.out.println("EVALUATING Select - ");
		System.out.println(exp.toString());
		return source.readOneTuple();
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#reset()
	 */
	public void reset() {
		// TODO Auto-generated method stub

	}
	
	public String toString(){
		
		return "EVALUATE Select - "+ this.exp;
	}
	
	public Operator peekNextOp(){
		return this.source;
	}
}
