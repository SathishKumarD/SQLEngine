package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class UnionOperator implements Operator {
	List<Operator> operators;
	
	@Override
	public ArrayList<Tuple> readOneTuple() {
		// TODO Auto-generated method stub
		for (Operator op : operators){
			ArrayList<Tuple> res = op.readOneTuple();
			if (res != null){
				return res;
			}
		}
		return null;
	}
	
	@Override
	public void reset() {
		// TODO Auto-generated method stub
	}

	@Override
	public Operator peekNextOp() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		// TODO Auto-generated method stub
		return this.operators.get(0).getOutputTupleSchema();
	}
	
	public UnionOperator(){
		this.operators = new ArrayList<Operator>();
	}
	
	public void addOperator(Operator op) {
		this.operators.add(op);		
	}
}
