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
	private Operator parentOperator;
	
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
		
		setChildOp(left);
		setRightOp(right);
		
	}
	
	@Override
	public ArrayList<Tuple> readOneTuple() {
		return null;
	}

	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		// TODO Auto-generated method stub
		return outputSchema;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		generateOutputSchema();
	}

	@Override
	public void setChildOp(Operator child) {		
		this.left = child;		
		left.setParent(this);
		//reset();
	}
	
	@Override
	public Operator getChildOp() {
		// TODO Auto-generated method stub
		return this.left;
	}
	
	public void setRightOp(Operator child){
		this.right = child;
		right.setParent(this);
		if(this.left != null) reset();
	}
	
	public Operator getLeftOperator()
	{
		return left;
	}
	
	public Operator getRightOperator()
	{
		return right;
	}

	@Override
	public Operator getParent() {
		return this.parentOperator;
	}

	@Override
	public void setParent(Operator parent) {
		this.parentOperator = parent;		
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
		
		StringBuilder b = new StringBuilder("JOIN WITH \n");
		Operator childOfRightBranch = this.right;
		
		while(childOfRightBranch != null)
		{
			b.append('\t' +childOfRightBranch.toString() + '\n');
			childOfRightBranch = childOfRightBranch.getChildOp();
		}
		
		return b.toString();
	}

}
