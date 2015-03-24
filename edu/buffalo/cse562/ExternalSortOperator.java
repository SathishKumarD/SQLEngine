package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import net.sf.jsqlparser.statement.select.OrderByElement;

public abstract class ExternalSortOperator implements Operator {
	Operator child;
	File swapDir;
	String sortField;
	Comparator<ArrayList<Tuple>> comp;
	int bufferLength;
	HashMap<String, ColumnDetail> outputSchema;
	private static final int BUFFER_SIZE = 3000;
	TreeMap<Integer, String> typeMap;
	List<ArrayList<Tuple>> workingSet;

	
	public ExternalSortOperator(Operator child, List<OrderByElement> OrderByElements) {
		// TODO Auto-generated constructor stub
		System.err.println(" Total Memory available is " + Runtime.getRuntime().totalMemory()/1000000 + "MB");
		swapDir = new File(ConfigManager.getSwapDir(), UUID.randomUUID().toString());
		if (!swapDir.exists()){
			swapDir.mkdir();
		}
		this.outputSchema = child.getOutputTupleSchema();
		
		String field = getFullField(OrderByElements.get(0).toString());
		this.child = child;
		this.sortField = field;
		
		this.comp = new TupleComparator(this.outputSchema.get(field).getIndex());
		this.bufferLength = Math.floorDiv(BUFFER_SIZE, this.getOutputTupleSchema().size());
		this.typeMap = new TreeMap<Integer, String>();
		this.workingSet = new ArrayList<ArrayList<Tuple>>(this.bufferLength);
		
		for (ColumnDetail c : outputSchema.values()){
			typeMap.put(c.getIndex(), c.getColumnDefinition().getColDataType().toString().toLowerCase());
		}
	}

	@Override
	public ArrayList<Tuple> readOneTuple() {
		// TODO Auto-generated method stub
		ArrayList<Tuple> currentTuple;
		
		// First run; sorts input tuples in batches, and writes to separate files on disk
		int index = 0;
		int nPass = 0;
		File currentFileHandler = getFileHandle(index, nPass);
		while((currentTuple = child.readOneTuple())!= null){
			if (addToSet(currentTuple, true, currentFileHandler)){
				currentFileHandler = getFileHandle(index++, nPass);
			}
		}
		// Recursive merging
		
		
		//##########	
		//Finally, return tuples from sorted file
		try {
			MiniScan finalOutput = new MiniScan(currentFileHandler, typeMap);
			while((currentTuple = finalOutput.readTuple())!= null){
				return currentTuple;
			}		
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}				
		return null;
	}
//	
//	private void mergeRecursive(int nPass, int start, int end){
//		if (end == start){
//			// The End
//			
//		}
//		int midPoint = ((start + end))/2;
//		if ((end - start) == 1){
//			File fname1 = getFileHandle(start, nPass);
//			File fname2 = getFileHandle(end, nPass);
//			File outputHandler = getFileHandle(midPoint, nPass + 1) ;
////			System.out.println("Merging " + fname1);
////			System.out.println("AND " + fname2);
//			mergeOnce(fname1, fname2, outputHandler);
//		}
//		else{ 
//			mergeRecursive(nPass, start, midPoint);
//			mergeRecursive(nPass, midPoint+1, end);
//		}
//	}
	
	private void mergeOnce(File fname1, File fname2, File currentFileHandler){
		try {
			MiniScan left = new MiniScan(fname1, typeMap);
			MiniScan right = new MiniScan(fname2, typeMap);
			ArrayList<Tuple> leftTup = left.readTuple();
			ArrayList<Tuple> rightTup = right.readTuple();
			
			//Merge procedure
			while (!(leftTup == null) && !(rightTup == null)){
				if (this.comp.compare(leftTup, rightTup) < 0){
//					System.out.println("adding left" + leftTup);
					addToSet(leftTup, false, currentFileHandler);
					leftTup = left.readTuple(); 
				}
				else{
//					System.out.println("adding right" + rightTup);
					addToSet(rightTup, false, currentFileHandler);
					rightTup = right.readTuple();
				}
			}
			
			//dump remainder into the working set and flush
			while (leftTup != null){
//				System.out.println("flushing left");
				addToSet(leftTup, false, currentFileHandler);
				leftTup = left.readTuple();
//				System.out.println(leftTup);
			}
			while (rightTup != null){
				System.out.println("flushing right");
				addToSet(rightTup, false, currentFileHandler);
				rightTup = right.readTuple();
			}
			
			writeToDisk(workingSet, currentFileHandler);
			System.gc();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	private boolean addToSet(ArrayList<Tuple> toAdd, boolean sort, File currentFileHandle){
		/* adds a tuple to the working set and also returns a flag indicating
		 * whether or not the working set was flushed (this is particularly useful for updating
		 * file indexes)
		 */
		if (workingSet.size() < this.bufferLength){
			workingSet.add(toAdd);
			return false;
		}
		else{
			if (sort){
				Collections.sort(workingSet, this.comp);
			}
//			System.err.println(" Collecting garbage with " 
//					+(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())/1000000 +"MB currently used");
			writeToDisk(workingSet, currentFileHandle);
			workingSet = new ArrayList<ArrayList<Tuple>>(this.bufferLength);
			workingSet.add(toAdd);	
			System.gc();
			return true;
		}
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		// TODO Auto-generated method stub
		return child.getOutputTupleSchema();
	}
	
	private boolean writeToDisk(List<ArrayList<Tuple>> out, File writeDir){
		PrintWriter pw;	
		
		try {			
			//append to file; useful for merging, and ensures that there is never a filenotfound exception
			pw = new PrintWriter(new FileWriter(writeDir, true));
			for(ArrayList<Tuple> t : out){
				Util.printToStream(t, pw);
			}
			pw.close();
			return true;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
	private File getFileHandle(int index, int nPass){
		String fname = nPass+"-"+index;
		File writeDir = new File(this.swapDir, fname);
		
		if (!writeDir.exists()){
			try {
				writeDir.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return writeDir;
	}
	
	private String getFullField(String input){
		for (Map.Entry<String, ColumnDetail> entry : this.outputSchema.entrySet()){
			ColumnDetail col = entry.getValue();
			if (col.getColumnDefinition().getColumnName().equals(input)){
				return entry.getKey();
			}
		}
		return null;
	}
	
	private class TupleComparator implements Comparator<ArrayList<Tuple>>{
		int fieldIndex;
		public TupleComparator(int fieldIndex){
			this.fieldIndex = fieldIndex;
		}

		@Override
		public int compare(ArrayList<Tuple> o1,
				ArrayList<Tuple> o2) {
			// TODO Auto-generated method stub
			return o1.get(this.fieldIndex).compareTo(o2.get(this.fieldIndex));
		}
	}
	
	
	private class MiniScan{
		BufferedReader br;
		String line;
		TreeMap<Integer, String> typeMap;

		public MiniScan(File filename, TreeMap<Integer, String> typeMap) throws IOException{
			Charset charset = Charset.forName("US-ASCII");
			this.br = Files.newBufferedReader(filename.toPath(), charset);
			this.typeMap = typeMap;
		}
		
		private ArrayList<Tuple> parseLine(String raw){
			String col[] = line.split("\\|");	
			ArrayList<Tuple> tuples = new ArrayList<Tuple>();
			for(Map.Entry<Integer, String> entry : typeMap.entrySet()) {
				tuples.add(new Tuple(entry.getValue(), col[entry.getKey()]));	
			}
			return tuples;
		}
		
		public ArrayList<Tuple> readTuple(){
			try {
				if ((line = br.readLine())!= null){
					return parseLine(line);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				this.br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;			
		}
	}
	

}
