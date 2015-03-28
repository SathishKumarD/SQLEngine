package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import net.sf.jsqlparser.expression.Expression;

public class HybridJoinOperator extends JoinOperator{
	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */
	
	private HashMap<String, List<ArrayList<Tuple>>> joinHash;
	Iterator<ArrayList<Tuple>> currentBag;
	boolean hashed = false;

	public HybridJoinOperator(Operator left, Operator right, Expression expr){
		super(left, right, expr);		
		joinHash = new HashMap<String, List<ArrayList<Tuple>>>();
		currentBag = new ArrayList<ArrayList<Tuple>>().iterator();
	}
	
	@Override
	public ArrayList<Tuple> readOneTuple() {
		// TODO Auto-generated method stub
//		leftTuple = left.readOneTuple();
		
		if (!hashed){
			long start = new Date().getTime();
			rightTuple = right.readOneTuple();		
			while(rightTuple != null){
//				System.out.println("***** Opening");
				String key = rightTuple.get(rightIndex).toString();
				List<ArrayList<Tuple>> prev = joinHash.get(key);
//				System.out.println("current tuples " +prev);
				if (prev == null){
					prev = new ArrayList<ArrayList<Tuple>>();
					joinHash.put(key, prev);
				}
				prev.add(rightTuple);
				rightTuple = right.readOneTuple();
			}
			hashed = true;
			// System.out.println("==== Hashed in " + ((float) (new Date().getTime() - start)/ 1000) + "s");
		}
		
		//try to read more
		if (!currentBag.hasNext()){		
			leftTuple = left.readOneTuple();
			while (leftTuple != null){
				String key = leftTuple.get(leftIndex).toString();
				List<ArrayList<Tuple>> hashedRight = joinHash.get(key);
				if (hashedRight != null){
					currentBag = hashedRight.iterator();
					break;
				}
				leftTuple = left.readOneTuple();
			}
		}
		
		if (leftTuple == null){
			if (currentBag.hasNext()) {
				if (leftTuple.size() > this.divider){
					leftTuple = (ArrayList<Tuple>) leftTuple.subList(0, divider);
				}
				leftTuple.addAll(currentBag.next());
				return leftTuple;
			}
		}
		return null;
	}
	
	@Override
	public void reset(){
		super.reset();
	}
}
