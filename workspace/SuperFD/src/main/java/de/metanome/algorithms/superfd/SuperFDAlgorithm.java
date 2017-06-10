package de.metanome.algorithms.superfd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import de.metanome.algorithms.superfd.ColumnCombinationBitset;
import de.metanome.algorithm_helper.data_structures.PositionListIndex;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.FunctionalDependencyResultReceiver;
import de.metanome.algorithm_integration.result_receiver.UniqueColumnCombinationResultReceiver;
import de.metanome.algorithm_integration.results.FunctionalDependency;
import de.metanome.algorithm_integration.results.UniqueColumnCombination;
import it.unimi.dsi.fastutil.longs.LongArrayList;

public class SuperFDAlgorithm {
	
	protected RelationalInputGenerator inputGenerator = null;
	protected FunctionalDependencyResultReceiver resultReceiver = null;
	
	protected String relationName;
	protected List<String> columnNames;
	protected List<PositionListIndex> plis;
	protected List<Boolean> is_one_value;
	
	List<List<String>> records;
	
	public void execute() throws AlgorithmExecutionException {

		this.initialize();
		records = this.readInput();
		this.print(records);
		
		genPLIs();
		//List<Pair<ColumnCombinationBitset, Integer>> fds = new ArrayList<Pair<ColumnCombinationBitset, Integer>>();
		List<FunctionalDependency> results = new ArrayList<FunctionalDependency>();
		
		ColumnCombinationBitset emptySet = new ColumnCombinationBitset();
		List<List<ColumnCombinationBitset>> L = new ArrayList<List<ColumnCombinationBitset>>();
		List<ColumnCombinationBitset> L0 = new ArrayList<ColumnCombinationBitset>();
		L0.add(emptySet);
		L.add(L0);
		ColumnCombinationBitset R = new ColumnCombinationBitset();
		HashMap<ColumnCombinationBitset, ColumnCombinationBitset> C = new HashMap<ColumnCombinationBitset, ColumnCombinationBitset>();
		
		List<ColumnCombinationBitset> L1 = new ArrayList<ColumnCombinationBitset>();
		for(int columnId = 0; columnId < this.columnNames.size(); ++columnId)
		{
			R.addColumn(columnId);
			L1.add(new ColumnCombinationBitset(columnId));
		}
		C.put(emptySet, R);
		L.add(L1);
		int l = 1;
		while(!L.get(l).isEmpty()) {
			// compute deps (Ll)
			List<ColumnCombinationBitset> L_l = L.get(l);
			for (ColumnCombinationBitset X : L_l) {
				ColumnCombinationBitset c_plus = R;
				for (int c_index : X.getSetBits()) {
					c_plus = c_plus.intersect(C.get(new ColumnCombinationBitset(X).removeColumn(c_index)));
				}
				C.put(X, c_plus);
			}
			for (ColumnCombinationBitset X : L_l) {
				ColumnCombinationBitset c_plus = C.get(X);
				for (int c_index : X.intersect(c_plus).getSetBits()) {
					ColumnCombinationBitset determant = new ColumnCombinationBitset(X).removeColumn(c_index);
					if (isFD(determant, c_index)) {
						results.add(createFD(determant, c_index));
						c_plus = c_plus.removeColumn(c_index).intersect(X);
						C.put(X, c_plus);
					}
				}
			}
			
			// prune (Ll)
			for (Iterator<ColumnCombinationBitset> iterator = L_l.iterator(); iterator.hasNext();) {
				ColumnCombinationBitset current = iterator.next();
				if (C.get(current).isEmpty()) {
					iterator.remove();
				}
			}
			
			if (L_l.isEmpty()) {
				break;
			}
			
			// Ll+1 := gen next level
			HashSet<ColumnCombinationBitset> l_plus_1 = new HashSet<ColumnCombinationBitset>();
			Collections.sort(L_l);
			ListIterator<ColumnCombinationBitset> iter = L_l.listIterator();
			ColumnCombinationBitset first = iter.next();
			while (iter.hasNext()) {
				ColumnCombinationBitset current_prefix = first.newWithoutLastColumn();
				List<ColumnCombinationBitset> same_prefix = new ArrayList<ColumnCombinationBitset>(Arrays.asList(first));
				while(iter.hasNext()) {
					ColumnCombinationBitset current = iter.next();
					if (current.newWithoutLastColumn().equals(current_prefix)) {
						same_prefix.add(current);
					} else {
						first = current;
						break;
					}
				}
				
				ListIterator<ColumnCombinationBitset> o_iter = same_prefix.listIterator();
				for (int i = 0; i < same_prefix.size(); ++i) {
					ColumnCombinationBitset o_current = o_iter.next();
					ListIterator<ColumnCombinationBitset> i_iter = same_prefix.listIterator();
					for (int j = 0; j < i; ++j) {
						l_plus_1.add(o_current.union(i_iter.next()));
					}
				}
			}
			
			L.add(new ArrayList<ColumnCombinationBitset>(l_plus_1));
			++l;
		}
		
		this.emit(results);
	}
	
	protected void initialize() throws InputGenerationException, AlgorithmConfigurationException {
		RelationalInput input = this.inputGenerator.generateNewCopy();
		this.relationName = input.relationName();
		this.columnNames = input.columnNames();
	}
	
	protected void genPLIs() {
		this.plis = new ArrayList<PositionListIndex>(this.columnNames.size());
		List<HashMap<String, LongArrayList>> m = new ArrayList<HashMap<String, LongArrayList>>(this.columnNames.size());
		for (int i = 0; i < this.columnNames.size(); ++i) {
			m.add(new HashMap<String, LongArrayList>());
		}
		ListIterator<List<String>> row_iter = records.listIterator();
		while (row_iter.hasNext())
		{
			long row_id = (long) row_iter.nextIndex();
			ListIterator<String> iter = row_iter.next().listIterator();
			while (iter.hasNext()) {
				HashMap<String, LongArrayList> cur_m = m.get(iter.nextIndex());
				String val = iter.next();
			    if (cur_m.containsKey(val)) {
			    	cur_m.get(val).add(row_id);
			    } else {
			    	cur_m.put(val, new LongArrayList(Arrays.asList(row_id)));
			    }
			}
		}
		ListIterator<HashMap<String, LongArrayList>> col_iter = m.listIterator();
		this.is_one_value = new ArrayList<Boolean>();
		while (col_iter.hasNext())
		{
			Collection<LongArrayList> sets = col_iter.next().values();
			this.is_one_value.add(sets.size() == 1);
			Iterator<LongArrayList> iter = sets.iterator();
			while (iter.hasNext()) {
				if (iter.next().size() <= 1) {
					iter.remove();
				}
			}
			this.plis.add(new PositionListIndex(new ArrayList<LongArrayList>(sets)));
		}
	}
	
	protected boolean isFD(ColumnCombinationBitset combination, int dependant)
	{
		return this.isFD(combination.getSetBits(), dependant);
	}
	
	protected boolean isFD(List<Integer> columnIds, int dependant)
	{
		if (columnIds.isEmpty()) {
			return this.is_one_value.get(dependant);
		}
		Iterator<Integer> itr = columnIds.iterator();
		PositionListIndex cur = this.plis.get(itr.next());
		while(itr.hasNext()) {
			cur = cur.intersect(this.plis.get(itr.next()));
			Iterator<LongArrayList> iter = cur.getClusters().iterator();
			while (iter.hasNext()) {
				if (iter.next().size() <= 1) {
					iter.remove();
				}
			}
		}
		long current_size = cur.size();
		cur = cur.intersect(this.plis.get(dependant));
		Iterator<LongArrayList> iter = cur.getClusters().iterator();
		while (iter.hasNext()) {
			if (iter.next().size() <= 1) {
				iter.remove();
			}
		}
		return current_size == cur.size();
	}
	
	protected boolean isUnique(ColumnCombinationBitset combination)
	{
		return this.isUnique(combination.getSetBits());
	}
	
	protected boolean isUnique(List<Integer> columnIds)
	{
		if (columnIds.size() == 0) {
			return false;
		}
		Iterator<Integer> itr = columnIds.iterator();
		PositionListIndex cur = this.plis.get(itr.next());
		while(itr.hasNext()) {
			cur = cur.intersect(this.plis.get(itr.next()));
			Iterator<LongArrayList> iter = cur.getClusters().iterator();
			while (iter.hasNext()) {
				if (iter.next().size() <= 1) {
					iter.remove();
				}
			}
		}
		return cur.size() == 0;
	}
	
	protected UniqueColumnCombination columnsAsUCC(ColumnCombinationBitset combination)
	{
		return this.columnsAsUCC(combination.getSetBits());
	}
	
	protected UniqueColumnCombination columnsAsUCC(List<Integer> columnIds)
	{
		ColumnIdentifier[] identifiers = new ColumnIdentifier[columnIds.size()];
		
		int i = 0;
		for (int id : columnIds) {
			identifiers[i] = (this.getColumnIdentifierForColumnId(id));
			i++;
		}
		
		UniqueColumnCombination combination = new UniqueColumnCombination(identifiers);
		return combination;
	}
	
	protected FunctionalDependency createFD(ColumnCombinationBitset combination, int dependant)
	{
		return new FunctionalDependency(combination.createColumnCombination(this.relationName, this.columnNames), this.getColumnIdentifierForColumnId(dependant));
	}
	
	protected List<List<String>> readInput() throws InputGenerationException, AlgorithmConfigurationException, InputIterationException {
		List<List<String>> records = new ArrayList<>();
		RelationalInput input = this.inputGenerator.generateNewCopy();
		while (input.hasNext())
			records.add(input.next());
		return records;
	}
	
	protected void print(List<List<String>> records) {
		// Print schema
		System.out.print(this.relationName + "( ");
		for (String columnName : this.columnNames)
			System.out.print(columnName + " ");
		System.out.println(")");
		
		// Print records
		for (List<String> record : records) {
			System.out.print("| ");
			for (String value : record)
				System.out.print(value + " | ");
			System.out.println();
		}
	}
	
	protected List<UniqueColumnCombination> generateResults() {
		List<UniqueColumnCombination> results = new ArrayList<>();
		ColumnCombination lhs = new ColumnCombination(this.getRandomColumn(), this.getRandomColumn());
		UniqueColumnCombination od = new UniqueColumnCombination(lhs);
		results.add(od);
		return results;
	}
	
	protected ColumnIdentifier getRandomColumn() {
		Random random = new Random(System.currentTimeMillis());
		return new ColumnIdentifier(this.relationName, this.columnNames.get(random.nextInt(this.columnNames.size())));
	}
	
	protected ColumnIdentifier getColumnIdentifierForColumnName(String columnName)
	{
		return new ColumnIdentifier(this.relationName, columnName);
	}
	
	protected ColumnIdentifier getColumnIdentifierForColumnId(int columnId)
	{
		return new ColumnIdentifier(this.relationName, this.columnNames.get(columnId));
	}
	
	protected void emit(List<FunctionalDependency> results) throws CouldNotReceiveResultException, ColumnNameMismatchException {
		for (FunctionalDependency fd : results)
			this.resultReceiver.receiveResult(fd);
	}
	
	@Override
	public String toString() {
		return this.getClass().getName();
	}
}
