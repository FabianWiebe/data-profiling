package de.metanome.algorithms.superucc;

public class Subrow {
	
	public String[] values;
//	public boolean hasNull = false;
	
	public Subrow(String... vals)
	{
		values = vals;
//		for (String val : vals) {
//			if (val == null) {
//				hasNull = true;
//				break;
//			}
//		}
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
		
		if(this.values.length != rhs.values.length)
			return false;
		
		for(int i = 0; i < this.values.length; i++)
		{
			// null != null is checked at creation
			if(rhs.values[i] == null || this.values[i] == null || !this.values[i].equals(rhs.values[i]))
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
			result = prime * result + ((value == null) ? 0 : value.hashCode());
		}
		
		return sum;
	}

}
