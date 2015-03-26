package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;

import net.sf.jsqlparser.expression.Expression;

public class HybridJoinOperator extends JoinOperator{
	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */
	
	private HashMap<String, ArrayList<Tuple>> joinHash;
	boolean hashed = false;

	public HybridJoinOperator(Operator left, Operator right, Expression expr){
		super(left, right, expr);		
		joinHash = new HashMap<String, ArrayList<Tuple>>();
	}
	
	@Override
	public ArrayList<Tuple> readOneTuple() {
		// TODO Auto-generated method stub
//		leftTuple = left.readOneTuple();
		
		if (!hashed){
			long start = new Date().getTime();
			rightTuple = right.readOneTuple();		
	
			while(rightTuple != null){
				joinHash.put(rightTuple.get(rightIndex).toString(), rightTuple);
				rightTuple = right.readOneTuple();
			}
			hashed = true;
			System.out.println("==== Hashed in " + ((float) (new Date().getTime() - start)/ 1000) + "s");
		}
		
		leftTuple = left.readOneTuple();
		while (leftTuple != null){
			ArrayList<Tuple> hashedRight = joinHash.get(leftTuple.get(leftIndex).toString());
			if (hashedRight != null){
				leftTuple.addAll(hashedRight);
				return leftTuple;
			}
			leftTuple = left.readOneTuple();
		}
		return null;
	}
}
