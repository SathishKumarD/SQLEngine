package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import net.sf.jsqlparser.expression.Expression;

public class JoinOperator implements Operator {
	protected Operator left;
	protected Operator right;
	protected HashMap<String, ColumnDetail> outputSchema = null;
	protected HashMap<String, ColumnDetail> leftSchema;
	protected HashMap<String, ColumnDetail> rightSchema;
	protected ArrayList<Tuple> leftTuple;
	protected ArrayList<Tuple> rightTuple;
	protected Expression expr;
	protected int leftIndex;
	protected int rightIndex;
	
	public JoinOperator(Operator left, Operator right, Expression expr){
		this.left = left;
		this.right = right;
		this.expr = expr;
		
		String[] fields = expr.toString().split("=");
		
		//Test left, then right
		ColumnDetail cd = left.getOutputTupleSchema().get(fields[0].trim());
		if (cd == null){
			cd = Evaluator.getColumnDetail(left.getOutputTupleSchema(), fields[1].trim().toLowerCase());
			leftIndex = cd.getIndex();
			rightIndex =  Evaluator.getColumnDetail(right.getOutputTupleSchema(),fields[0].trim().toLowerCase()).getIndex();
		}
		
		else{
			rightIndex = Evaluator.getColumnDetail(right.getOutputTupleSchema(), fields[1].trim()).getIndex();
		}		
		
		generateOutputSchema();
	}
	
	@Override
	public ArrayList<Tuple> readOneTuple() {
		return null;
	}

	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

	@Override
	public Operator getChildOp() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setChildOp(Operator child) {
		// TODO Auto-generated method stub

	}

	@Override
	public Operator getParent() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setParent(Operator parent) {
		// TODO Auto-generated method stub

	}
	
	private void generateOutputSchema(){
		outputSchema = new HashMap<String, ColumnDetail>();
		leftSchema = new HashMap<String, ColumnDetail>(left.getOutputTupleSchema());
		rightSchema = new HashMap<String, ColumnDetail>(right.getOutputTupleSchema());
		int offset = 0;
		for (Entry<String, ColumnDetail> en : rightSchema.entrySet()){
			String key = en.getKey();
			ColumnDetail value = en.getValue().clone();
			int index = value.getIndex();
			if (index > offset){
				offset = index;
			}
			outputSchema.put(key, value);
		}
		for (Entry<String, ColumnDetail> en : leftSchema.entrySet()){
			String key = en.getKey();
			ColumnDetail value = en.getValue().clone();
			int index = value.getIndex();
			value.setIndex(index + offset + 1);
			outputSchema.put(key, value);
		}
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "Join on " + this.expr;
	}

}
