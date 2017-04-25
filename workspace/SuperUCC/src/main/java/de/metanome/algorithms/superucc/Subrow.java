package de.metanome.algorithms.superucc;

public class Subrow {
	
	public String[] values;
	
	public Subrow(String... vals)
	{
		values = vals;
	}
	
	@Override
	public boolean equals(Object o)
	{
		if(!(o instanceof Subrow))
		{
			return false;
		}
		
		if(o == this)
			return true;
		
		Subrow rhs = (Subrow) o;
		
		if(rhs.values.length != rhs.values.length)
			return false;
		
		for(int i = 0; i < this.values.length; i++)
		{
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
		int sum = 0;
		for(String value : this.values)
		{
			sum += value.hashCode();
		}
		
		return sum;
	}

}
