package de.metanome.algorithms.superid;

public class DataTuple {
	
	public String[] values;
	
	public DataTuple(String... vals)
	{
		values = vals;
	}
	
	@Override
	public boolean equals(Object o)
	{
		if(!(o instanceof DataTuple))
		{
			return false;
		}
		
		if(o == this)
			return true;
		
		DataTuple rhs = (DataTuple) o;
		
		if(this.values.length != rhs.values.length)
			return false;
		
		for(int i = 0; i < this.values.length; i++)
		{
			// null != null is checked before creation
			if(!this.values[i].equals(rhs.values[i]))
			{
				return false;
			}
		}
		
		return true;
	}
	
	@Override
	public int hashCode()
	{
	    final int prime = 31;
	    int result = 1;
	    
		int sum = 0;
		for(String value : this.values)
		{
			// null != null is checked  before creation
			result = prime * result + value.hashCode();
		}
		
		return sum;
	}

}
