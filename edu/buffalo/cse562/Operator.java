/**
 * 
 */
package edu.buffalo.cse562;

/**
 * @author Sathish
 *
 */
public interface Operator {
	
	public Datum[] readOneTuple();
	public void reset();
	public Operator peekNextOp();
}
