/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;

/**
 * @author Sathish
 *
 */
public class JoinOperator implements Operator {

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */
	
	//TODO: Create setters and gettersa
	
	public ArrayList<Operator> sources;
	
	@Override
	public Datum[] readOneTuple() {
		// TODO Auto-generated method stub
		
		System.out.println("sources has "+ sources.size() + "items");
		System.out.println("Evaluating read one tuple on join");
		
		
		// temporary code
		return sources.get(0).readOneTuple();
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#reset()
	 */
	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

	public JoinOperator(Operator base){
		sources = new ArrayList<Operator>();
		sources.add(base);
	}
	
	public String toString(){
		StringBuilder b = new StringBuilder("JOIN [ \n");
		for (Operator op : sources){
			b.append('\t' +op.toString() + '\n');
		}
		b.append(']');
		return b.toString();
	}
	
	public Operator peekNextOp(){
		return this.sources.get(0);
	}
}
