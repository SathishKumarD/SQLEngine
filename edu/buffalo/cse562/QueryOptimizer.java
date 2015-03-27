package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class QueryOptimizer {

	public QueryOptimizer()
	{

	}

	public QueryOptimizer(Operator current)
	{
		current = pushSelection(current);
		current = replaceOperators(current);		
	}	
	
	public Operator pushSelection(Operator current)
	{
		Operator currOperator = current;
		Operator parentOperator = current;		
		
		do{				
			if(currOperator instanceof SelectionOperator)
			{		
				Operator op_modifiedTree = pushDownSelection((SelectionOperator)currOperator);					
				
				if(op_modifiedTree != null){
					currOperator = op_modifiedTree;
				}
			}	
				parentOperator = currOperator;
				currOperator = parentOperator.getChildOp();
		}
		while(currOperator != null);
		
		Operator root = getRoot(parentOperator);
		
		return root;
	}	
			
	//returns the root of the tree
	private Operator getRoot(Operator currOperator) {		
		while(currOperator.getParent()!=null)
		{
			currOperator = currOperator.getParent();
		}	
		return currOperator;
	}
	
	/***
	 * Replaces Selection sitting on CrossProducts With Hash Joins
	 * Replaces Group By WIth GroupBy on top of External Sort 
	 * @param root of the unoptimized expression tree
	 * @return root of the new Expression Tree
	 */
	public Operator replaceOperators(Operator current)
	{
		Operator currOperator = current;
		Operator parentOperator = current;		
		
		do{				
			if(currOperator instanceof SelectionOperator)
			{		
				Operator op_modifiedTree = patternMatchSelectionOnCrossProduct((SelectionOperator)currOperator);					
				
				if(op_modifiedTree != null){
					currOperator = op_modifiedTree;
				}
			}
			
			if(currOperator instanceof GroupByOperator)
			{
				//replaceGroupBy((GroupByOperator)currOperator);				
			}			
			
			parentOperator = currOperator;
			currOperator = parentOperator.getChildOp();
		}
		while(currOperator != null);
		
		Operator root = getRoot(parentOperator);
		
		return root;
	}	
			
	//iteratively keep sending the input selection down to pattern match cross products sitting below
	//until select conditions are empty or until the last child in the tree
	private Operator patternMatchSelectionOnCrossProduct(SelectionOperator selectionOperator)
	{
		Operator oldSelectOp = selectionOperator;				
		Operator modifiedSelect = null;		
				
		do{
			if(modifiedSelect != null) oldSelectOp = modifiedSelect;
			 
			modifiedSelect = replaceCrossProductWithHashJoin((SelectionOperator)oldSelectOp);
			
			if(!(modifiedSelect instanceof SelectionOperator)) 
			{
				return modifiedSelect;
			}
			
		}while(!modifiedSelect.equals(oldSelectOp));
		
		return modifiedSelect;
	}
	
	//pushes Selection below cross product wherever applicable
	private Operator pushDownSelection(SelectionOperator selectionOperator)
	{
		if(selectionOperator == null || selectionOperator.getChildOp() == null) return null;

		List<Expression> exprList = splitANDClauses(selectionOperator.getExpression());
		Operator op = selectionOperator;

		do{
			if(op.getChildOp() instanceof CrossProductOperator)
			{
				CrossProductOperator joinOp = (CrossProductOperator) op.getChildOp();
				
				List<Expression> newSelLeftExpr_List = new LinkedList<Expression>(); 
				List<Expression> newSelRightExpr_List = new LinkedList<Expression>(); 
				
				populateNewSelectionList(newSelLeftExpr_List, newSelRightExpr_List, exprList, joinOp);
				
				if(newSelLeftExpr_List.size() > 0) 
				{
					Operator oper = new SelectionOperator(joinOp.getLeftOperator(), newSelLeftExpr_List);
					
					//left deep tree. left operator is the child! 
					joinOp.setChildOp(oper);
				}
				if(newSelRightExpr_List.size() > 0)
				{
					Operator oper = new SelectionOperator(joinOp.getRightOperator(), newSelRightExpr_List);
					
					joinOp.setRightOp(oper);
				}
				
				// if the selection has any conditions to enforce re-initialise it with modified expression list
				// if it does not have any conditions to enforce just rest the parent child relation of its parent
				if(exprList.size() > 0) 
				{
					selectionOperator.setSelectExpression(exprList);
					return selectionOperator;
				}
				else 
				{
					selectionOperator.getParent().setChildOp(selectionOperator.getChildOp());

					return selectionOperator.getParent();
				}
			}
						
			op = op.getChildOp();
		}while(op.getChildOp()!= null);

		return selectionOperator;
	}

	
	private void populateNewSelectionList(List<Expression> newSelLeftExpr_List, List<Expression> newSelRightExpr_List, List<Expression> selectionExprList, CrossProductOperator joinOp)
	{		
		for(Iterator<Expression> itr =  selectionExprList.iterator(); itr.hasNext();)
		{
			Expression expr = itr.next();	
			
			if(checkIfExpressionIsPushable(expr, joinOp, newSelLeftExpr_List, newSelRightExpr_List)) itr.remove();			
		}
	}
	
	private boolean checkIfExpressionIsPushable(Expression expr, CrossProductOperator joinOp, List<Expression> newSelLeftExpr_List, List<Expression> newSelRightExpr_List)
	{
		boolean isExistsInLeft = false;
		boolean isExistsInRight = false;
		
		if(expr instanceof BinaryExpression) 
		{
			//if both the left n right expr is a column and the table names are diff return false//TODO
			Expression leftExpr = ((BinaryExpression) expr).getLeftExpression();
			Expression rightExpr = ((BinaryExpression) expr).getRightExpression();
			
			if(checkIfExprRefersMoreThanOneRelation(leftExpr, rightExpr)) return false;
			
			if(leftExpr instanceof Column)
			{
				if(expressionExistsInSchema(((Column) leftExpr).getWholeColumnName(), joinOp.getLeftOperator().getOutputTupleSchema().keySet()))
					isExistsInLeft = true;
				if(expressionExistsInSchema(((Column) leftExpr).getWholeColumnName(), joinOp.getRightOperator().getOutputTupleSchema().keySet()))
					isExistsInRight = true;
			}
			
			if(rightExpr instanceof Column)
			{
				if(expressionExistsInSchema(((Column) rightExpr).getWholeColumnName(), joinOp.getLeftOperator().getOutputTupleSchema().keySet()))
					isExistsInLeft = true;
				if(expressionExistsInSchema(((Column) rightExpr).getWholeColumnName(), joinOp.getRightOperator().getOutputTupleSchema().keySet()))
					isExistsInRight = true;
			}
		}
		
		if(isExistsInLeft && isExistsInRight) return false;
		else if((isExistsInLeft && !(isExistsInRight)))
		{										
			newSelLeftExpr_List.add(expr);
			return true;
		}
		else if((isExistsInRight && !(isExistsInLeft)))
		{
			newSelRightExpr_List.add(expr);
			return true;
		}
		
		return false;
	}

	//returns true if expression refers more than a relation
	private boolean checkIfExprRefersMoreThanOneRelation(Expression leftExpr,Expression rightExpr)
	{
		if((leftExpr instanceof Column) && (rightExpr instanceof Column))
		{
			String leftCol = ((Column) leftExpr).getTable().getName();
			String rightCol = ((Column) rightExpr).getTable().getName();
			
			if(!(leftCol.equalsIgnoreCase(rightCol))) return true;				
		}
		
		return false;
	}
	/***
	 * conversion of cross prod to joins defined only on equi joins now in project 2
	 * @param selectionOperator
	 * @return
	 */
	private Operator replaceCrossProductWithHashJoin(SelectionOperator selectionOperator)
	{
		if(selectionOperator == null || selectionOperator.getChildOp() == null) return null;

		List<Expression> exprList = splitANDClauses(selectionOperator.getExpression());
		Operator childOperator = selectionOperator.getChildOp();
		
		do
		{			
			if(childOperator instanceof CrossProductOperator)
			{
				CrossProductOperator crossPOperator = (CrossProductOperator)childOperator;						
										
				for(Iterator<Expression> itr =  exprList.iterator(); itr.hasNext();)
				{
					Expression expr = itr.next();
										
					if(expr instanceof EqualsTo)
					{
						EqualsTo equalsExpr = (EqualsTo)expr;

						if(expressionMatchesJoinOp(equalsExpr, crossPOperator))
						{							
							HybridJoinOperator hashjoinOp = (new HybridJoinOperator(crossPOperator.getLeftOperator(), 
									crossPOperator.getRightOperator(), equalsExpr));
							// removes the equalTo expression before passing on to Selection Operator!


							 itr.remove();
							 childOperator.getParent().setChildOp(hashjoinOp);	
							 break;
						}
					}											
				}	
				//selectionOperator.setSelectExpression(exprList); //TODO
				// if the selection has any conditions to enforce re-initialise it with modified expression list
				// if it does not have any conditions to enforce just rest the parent child relation of its parent
				if(exprList.size() > 0) 
				{
					selectionOperator.setSelectExpression(exprList);
				}
				else 
				{
					selectionOperator.getParent().setChildOp(selectionOperator.getChildOp());
					
					return selectionOperator.getParent();
				}
			}	
			childOperator = childOperator.getChildOp();
		} while(childOperator != null);
		
		return selectionOperator;
	}
	
	/***
	 *  checks if expression macthes with a join operator
	 */
	private Boolean expressionMatchesJoinOp(EqualsTo equalsExpression, CrossProductOperator crossPop)
	{		
		HashMap<String, ColumnDetail> leftSchema = (crossPop.getLeftOperator()).getOutputTupleSchema();
		HashMap<String, ColumnDetail> rightSchema = (crossPop.getRightOperator()).getOutputTupleSchema();
		
		Expression left =  ((EqualsTo) equalsExpression).getLeftExpression();
		Expression right =  ((EqualsTo) equalsExpression).getRightExpression();
		
		if(left instanceof Column && right instanceof Column)
		{
			String leftExprName_Str = ((Column) left).getWholeColumnName();
			String rightExprName_Str = ((Column) right).getWholeColumnName();
			
			if(
					(
							expressionExistsInSchema(leftExprName_Str, leftSchema.keySet()) && 
							expressionExistsInSchema(rightExprName_Str, rightSchema.keySet())
					)
							|| 
					(		
							expressionExistsInSchema(leftExprName_Str, rightSchema.keySet()) && 
							expressionExistsInSchema(rightExprName_Str, leftSchema.keySet())
					)
			 )
			{
				return true;
			}					
		}
		
		return false;
	}

	/***
	 *  checks if column exists in Schema
	 */
	private boolean expressionExistsInSchema(String colName, Set<String> keySet) {
		for(String key : keySet)
		{
			if(colName.equalsIgnoreCase(key)) return true;
		}
		return false;
	}

	/***
	 * Splits and clauses to multiple and clauses
	 * @param e
	 * @return
	 */
	private List<Expression> splitANDClauses(Expression e) {
	  List<Expression> ret = new LinkedList<Expression>();
	     
	  if(e instanceof AndExpression){
	    AndExpression a = (AndExpression)e;
	    ret.addAll(
	    		splitANDClauses(a.getLeftExpression())
	    );
	    ret.addAll(
	    		splitANDClauses(a.getRightExpression())
	    );
	  } else {
	    ret.add(e);
	  }
	  
	  return ret;
	}
	
	//to be changed to external sort! : TODO to keno
	private void replaceGroupBy(GroupByOperator groupByOp)
	{
		List<Column> grpByExpressionsList = groupByOp.getGroupByColumns();
		List<OrderByElement> orderByElements = new ArrayList<OrderByElement>();
		
		for(Expression exp : grpByExpressionsList)
		{
			OrderByElement orderByElem = new OrderByElement();
			orderByElem.setExpression(exp);
			
			orderByElements.add(orderByElem);		
		}
		ExternalSortOperator externalSortOp = new ExternalSortOperator(groupByOp.getChildOp(), orderByElements);
		
		//System.out.println("settng child");
		groupByOp.setChildOp(externalSortOp);	
		//System.out.println("child set ");
	}
	
}
