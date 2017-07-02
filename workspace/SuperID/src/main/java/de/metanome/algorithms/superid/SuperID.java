package de.metanome.algorithms.superid;

import java.util.ArrayList;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.BooleanParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.IntegerParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.RelationalInputParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.StringParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementRelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;

public class SuperID extends SuperIDAlgorithm 				// Separating the algorithm implementation and the Metanome interface implementation is good practice
						  implements InclusionDependencyAlgorithm, 			// Defines the type of the algorithm, i.e., the result type, for instance, FunctionalDependencyAlgorithm or InclusionDependencyAlgorithm; implementing multiple types is possible
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
		return "Discovers all unary minimal inclusion dependencies";
	}
	
	@Override
	public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() { // Tells Metanome which and how many parameters the algorithm needs
		ArrayList<ConfigurationRequirement<?>> conf = new ArrayList<>();
		// An algorithm can ask for more than one input; this is typical for IND detection algorithms
		conf.add(new ConfigurationRequirementRelationalInput(SuperID.Identifier.INPUT_GENERATOR.name(),
		 ConfigurationRequirement.ARBITRARY_NUMBER_OF_VALUES));
		
		return conf;
	}

	@Override
	public void setStringConfigurationValue(String identifier, String... values) throws AlgorithmConfigurationException {
		if (SuperID.Identifier.SOME_STRING_PARAMETER.name().equals(identifier))
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
		if (SuperID.Identifier.SOME_INTEGER_PARAMETER.name().equals(identifier))
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
		if (SuperID.Identifier.SOME_BOOLEAN_PARAMETER.name().equals(identifier))
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
		if (!SuperID.Identifier.INPUT_GENERATOR.name().equals(identifier))
			this.handleUnknownConfiguration(identifier, values);
		this.inputGenerator = values;
		
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

	@Override
	public void setResultReceiver(InclusionDependencyResultReceiver resultReceiver) {
		this.resultReceiver = resultReceiver;
	}
}
