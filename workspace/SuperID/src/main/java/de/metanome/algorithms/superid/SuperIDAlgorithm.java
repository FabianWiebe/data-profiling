package de.metanome.algorithms.superid;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import de.metanome.algorithm_helper.data_structures.ColumnCombinationBitset;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;

public class SuperIDAlgorithm {
	
	protected RelationalInputGenerator[] inputGenerator = null;
	protected InclusionDependencyResultReceiver resultReceiver = null;
	
	protected List<String> relationNames = new LinkedList<String>();
	protected List<List<String>> columnNames = new LinkedList<List<String>>();
	protected HashMap<String, ColumnCombinationBitset> inv_index = new HashMap<String, ColumnCombinationBitset>();
	protected List<InclusionDependency> results = new LinkedList<InclusionDependency>();
	protected List<Integer> offsets = new ArrayList<Integer>();
	
	List<List<List<String>>> records;
	
	
	public void execute() throws AlgorithmExecutionException {

		this.initialize();
		records = this.readInput();
		System.out.println("Starting Super ID with data sets: " + String.join(", ", this.relationNames));
		int _offset = 0;
		
		// calculate offsets for column combination bitset
		for(List<String> columns : columnNames) {
			offsets.add(_offset);
			_offset += columns.size();
		}
		ColumnCombinationBitset full_bitset = new ColumnCombinationBitset();
		int last_table_index = this.offsets.size()-1;
		int total_columns = this.offsets.get(last_table_index) + this.columnNames.get(last_table_index).size();
		full_bitset.setAllBits(total_columns);
		genInvertedIndex();
		
		// initialize rhs
		ColumnCombinationBitset rhs[] = new ColumnCombinationBitset[total_columns];
		for (int i = 0; i < total_columns; ++i) {
			rhs[i] = new ColumnCombinationBitset(full_bitset);
		}
		
		// remove candidates
		for (ColumnCombinationBitset v : inv_index.values()) {
			for (int A : v.getSetBits()) {
				rhs[A] = rhs[A].intersect(v);
			}
		}
		
		// Create possbile column Permutations
		ColumnPermutation identifiers[] = new ColumnPermutation[total_columns];
		int itr = 0;
		for(int i = 0; i< relationNames.size(); ++i) {
			String relationName = relationNames.get(i);
			for (String column_name : columnNames.get(i)) {
				identifiers[itr++] = new ColumnPermutation(new ColumnIdentifier(relationName, column_name));
			}
		}
		
		// Generate output
		for (int A = 0; A < total_columns; ++A) {
			for (Integer B : rhs[A].removeColumn(A).getSetBits()) {
				this.results.add(new InclusionDependency(identifiers[A], identifiers[B]));
			}
		}
		
		this.emit(this.results);
	}
	
	protected void initialize() throws InputGenerationException, AlgorithmConfigurationException {
		for (RelationalInputGenerator generator : this.inputGenerator) {
			RelationalInput input = generator.generateNewCopy();
			this.relationNames.add(input.relationName());
			this.columnNames.add(input.columnNames());
		}
	}
	
	protected void genInvertedIndex() {
		ListIterator<Integer> offset_itr = this.offsets.listIterator();
		for (List<List<String>> table_data : records) {
			int offset = offset_itr.next();
			ListIterator<List<String>> row_iter = table_data.listIterator();
			while (row_iter.hasNext())
			{
				ListIterator<String> iter = row_iter.next().listIterator();
				while (iter.hasNext()) {
					int column_id = (int) iter.nextIndex();
					String val = iter.next();
					if (val == null) {
						continue;
					}
					if (this.inv_index.containsKey(val)) {
						this.inv_index.get(val).addColumn(offset + column_id);
					} else {
						this.inv_index.put(val, new ColumnCombinationBitset(offset + column_id));
					}
				}
			}
		}
	}
	
	protected List<List<List<String>>> readInput() throws InputGenerationException, AlgorithmConfigurationException, InputIterationException {
		List<List<List<String>>> all_records = new ArrayList<>();
		for (RelationalInputGenerator generator : this.inputGenerator) {
			List<List<String>> records = new ArrayList<>();
			RelationalInput input = generator.generateNewCopy();
			while (input.hasNext())
				records.add(input.next());
			all_records.add(records);
		}
		return all_records;
	}
	
	protected void emit(List<InclusionDependency> results) throws CouldNotReceiveResultException, ColumnNameMismatchException {
		for (InclusionDependency id : results) {
			this.resultReceiver.receiveResult(id);
		}
	}
	
	@Override
	public String toString() {
		return this.getClass().getName();
	}
}
