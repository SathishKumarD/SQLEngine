package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;
 
public class ExpressionTree {
	public Operator generateTree(SelectBody sel){
		Operator current = null;	
		PlainSelect select = (PlainSelect) sel;
		FromItem fi = select.getFromItem();
		Table table  = null;
		if (fi instanceof Table){
			table = (Table) fi;
			String tableName = (table).getWholeTableName();
			current = new ScanOperator(tableName);			
		}
		else if (fi instanceof SubSelect){
			 
		}		
		else if (fi instanceof SubJoin){

		}
		
		List<Join> joins = (List<Join>) select.getJoins();
		if (joins != null){
			if (joins.size() > 0){
				for (Join j : joins){
					FromItem fr = j.getRightItem();
					if (fr instanceof Table){
						current = new JoinOperator(current, new ScanOperator(((Table) fr).getName()), j.getOnExpression());
					}
				}
			}
		}		
		
		Expression exp = (Expression) select.getWhere();
		if (exp != null){
			current = new SelectionOperator(current, exp);
		}
		
		
		List<OrderByElement> OrderByElements  = (List<OrderByElement>)select.getOrderByElements();		
		if(OrderByElements != null && OrderByElements.size() > 0){
			current = new SortOperator(current, OrderByElements);
		}
		
		List<SelectItem> selItems = (List<SelectItem>) select.getSelectItems();
		if (selItems != null){
			if (selItems.size() > 0){				
						current = new ExtendedProjection(current, selItems);
			}
		}
		
//		List<OrderByElement> orderByElements = (List<OrderByElement>) select.getOrderByElements(); //TODO
//		if(orderByElements != null && orderByElements.size() > 0){
//			current = new SortOperator(current, OrderByElements);
//		}
		

		return current;
	}
}
