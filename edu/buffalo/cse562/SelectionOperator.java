/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

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
	Column[] schema;
	
	public SelectionOperator(Operator input, Expression exp, Column[] schema){
		this.source = input;
		this.exp = exp;
	}
	
	public ArrayList<Tuple> readOneTuple() {
		// TODO Auto-generated method stub
		
		System.out.println("EVALUATING Select - ");
		System.out.println(exp.toString());
		
		ArrayList<Tuple> tuple = null;
		do
		{
			tuple = source.readOneTuple();
			if(tuple==null) return null;
			
		}while(tuple==null);
		
		return tuple;
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
