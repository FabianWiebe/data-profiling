package de.metanome.algorithms.superucc;

import java.util.ArrayList;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.BooleanParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.IntegerParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.UniqueColumnCombinationsAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.RelationalInputParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.StringParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementBoolean;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementInteger;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementRelationalInput;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementString;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.UniqueColumnCombinationResultReceiver;

public class SuperUCC extends SuperUCCAlgorithm 				// Separating the algorithm implementation and the Metanome interface implementation is good practice
						  implements UniqueColumnCombinationsAlgorithm, 			// Defines the type of the algorithm, i.e., the result type, for instance, FunctionalDependencyAlgorithm or InclusionDependencyAlgorithm; implementing multiple types is possible
						  			 RelationalInputParameterAlgorithm,	// Defines the input type of the algorithm; relational input is any relational input from files or databases; more specific input specifications are possible
						  			 StringParameterAlgorithm, IntegerParameterAlgorithm, BooleanParameterAlgorithm {	// Types of configuration parameters this algorithm requires; all these are optional

	public enum Identifier {
		INPUT_GENERATOR, SOME_STRING_PARAMETER, SOME_INTEGER_PARAMETER, SOME_BOOLEAN_PARAMETER
	};

	@Override
	public String getAuthors() {
		return "Arne Mayer, Fabian Wiebe";
	}

	@Override
	public String getDescription() {
		return "Discovers all unique column combinations";
	}
	
	@Override
	public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() { // Tells Metanome which and how many parameters the algorithm needs
		ArrayList<ConfigurationRequirement<?>> conf = new ArrayList<>();
		conf.add(new ConfigurationRequirementRelationalInput(SuperUCC.Identifier.INPUT_GENERATOR.name()));
		
		return conf;
	}

	@Override
	public void setStringConfigurationValue(String identifier, String... values) throws AlgorithmConfigurationException {
		if (SuperUCC.Identifier.SOME_STRING_PARAMETER.name().equals(identifier))
		{
			//this.someStringParameter = values[0];
		}			
		else
		{
			this.handleUnknownConfiguration(identifier, values);
		}
	}

	@Override
	public void setIntegerConfigurationValue(String identifier, Integer... values) throws AlgorithmConfigurationException {
		if (SuperUCC.Identifier.SOME_INTEGER_PARAMETER.name().equals(identifier))
		{
			//this.someIntegerParameter = values[0];
		}
		else
		{
			this.handleUnknownConfiguration(identifier, values);
		}		
	}

	@Override
	public void setBooleanConfigurationValue(String identifier, Boolean... values) throws AlgorithmConfigurationException {
		if (SuperUCC.Identifier.SOME_BOOLEAN_PARAMETER.name().equals(identifier))
		{
			//this.someBooleanParameter = values[0];
		}
		else
		{
			this.handleUnknownConfiguration(identifier, values);
		}	
	}

	@Override
	public void setRelationalInputConfigurationValue(String identifier, RelationalInputGenerator... values) throws AlgorithmConfigurationException {
		if (!SuperUCC.Identifier.INPUT_GENERATOR.name().equals(identifier))
			this.handleUnknownConfiguration(identifier, values);
		this.inputGenerator = values[0];
	}

	@Override
	public void setResultReceiver(UniqueColumnCombinationResultReceiver resultReceiver) {
		this.resultReceiver = resultReceiver;
	}

	@Override
	public void execute() throws AlgorithmExecutionException {
		super.execute();
	}

	private void handleUnknownConfiguration(String identifier, Object[] values) throws AlgorithmConfigurationException {
		throw new AlgorithmConfigurationException("Unknown configuration: " + identifier + " -> [" + concat(values, ",") + "]");
	}
	
	private static String concat(Object[] objects, String separator) {
		if (objects == null)
			return "";
		
		StringBuilder buffer = new StringBuilder();
		for (int i = 0; i < objects.length; i++) {
			buffer.append(objects[i].toString());
			if ((i + 1) < objects.length)
				buffer.append(separator);
		}
		return buffer.toString();
	}
}
