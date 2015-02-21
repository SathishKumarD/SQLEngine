package edu.buffalo.cse562;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
 
public class ExpressionTree {
	public Operator generateTree(SelectBody sel){
		Operator current = null;	
		PlainSelect select = (PlainSelect) sel;		
		current = addScanOperator(current, select);
		current = addJoinOperator(current, select);
		current = addSelectionOperator(current, select);
		current = addExtendedProjectionOperator(current, select);
		current = addLimitOperator(current, select);
		return current;
	}
	
	private Operator addJoinOperator(Operator current,PlainSelect select)
	{
		List<Join> joins = (List<Join>) select.getJoins();
		if (joins != null){
			if (joins.size() > 0){
				for (Join j : joins){
					current = buildJoins(current, j);
				}
			}
		}
		return current;
	}
	
	private Operator addSelectionOperator(Operator current,PlainSelect select)
	{
		Expression exp = (Expression) select.getWhere();
		if (exp != null){
			current = new SelectionOperator(current, exp);
		}
		return current;
	}
	
	private Operator addExtendedProjectionOperator(Operator current,PlainSelect select)
	{
		List<SelectItem> selItems = (List<SelectItem>) select.getSelectItems();
		if (selItems != null){
			if (selItems.size() > 0){				
				current = new ExtendedProjection(current, selItems);
			}
		}
		return current;
	}
	private Operator addScanOperator(Operator current,PlainSelect select)
	{
		FromItem fi = select.getFromItem();
		Table table  = null;		
		if (fi instanceof Table){
			table = (Table) fi;
			String tableName = (table).getWholeTableName();
			current = new ScanOperator(tableName);			
		}
		else if (fi instanceof SubSelect){
			current = generateTree(((SubSelect) fi).getSelectBody());
		}		
		else if (fi instanceof SubJoin){
		}
		return current;
	}
	public Operator buildJoins(Operator current, Join j){
		FromItem fr = j.getRightItem();		
		if (fr instanceof Table){
			current = new JoinOperator(current, new ScanOperator(((Table) fr).getName()), j.getOnExpression());
		}	
		return current;
	}
	
	public Operator addLimitOperator(Operator current, PlainSelect select){
		Limit lim = (Limit) select.getLimit();		
		if (lim != null){
			return new LimitOperator(current, lim);
		}
		return null;
	}
}
