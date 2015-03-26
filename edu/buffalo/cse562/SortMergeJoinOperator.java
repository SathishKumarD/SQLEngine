package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.Comparator;

import net.sf.jsqlparser.expression.Expression;

public class SortMergeJoinOperator extends JoinOperator{
	Comparator<ArrayList<Tuple>> comp;
	boolean iterate = true;
	
	public SortMergeJoinOperator(Operator left, Operator right, Expression expr){
		super(left, right, expr);		
	}
	
	@Override
	public ArrayList<Tuple> readOneTuple() {
		if(iterate){
			leftTuple = left.readOneTuple();
			rightTuple = right.readOneTuple();
			iterate = false;
		}
		
		while (!(left == null) && !(right == null)){
			int diff = this.comp.compare(leftTuple, rightTuple);
			if (diff == 0){
				leftTuple.addAll(rightTuple);
				iterate = true;
				return leftTuple;
			}
			else if (diff < 0){
				leftTuple = left.readOneTuple();
			}
			else {
				rightTuple = right.readOneTuple();
			}
		}
		return null;
	}
	
	public void setComparator(Comparator<ArrayList<Tuple>> comp){
		this.comp = comp;
	}
}
