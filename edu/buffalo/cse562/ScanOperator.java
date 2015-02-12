/**
 * 
 */
package edu.buffalo.cse562;

import java.io.File;

/**
 * @author Sathish
 *
 */
public class ScanOperator implements Operator {

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */
	private File tableSource;
	
	public ScanOperator(String tableName){
		this.tableSource = new File (tableName);
	}
	
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
		return "SCAN TABLE " +tableSource.getName();
	}
	
	public Operator peekNextOp(){
		return null;
	}
}
