package entities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CeldaValueWritable implements WritableComparable<CeldaValueWritable> {
	// key
	private CeldaWritable celdaKey;
	// value
	private Double rend;

	public CeldaValueWritable() {
		celdaKey = new CeldaWritable();
		rend = new Double(0);
	}

	public CeldaValueWritable(CeldaWritable celda, double rend) {
		this.celdaKey = celda;
		this.rend = rend;
	}

	public CeldaWritable getCeldaKey() {
		return celdaKey;
	}

	public void setCeldaKey(CeldaWritable celdaKey) {
		this.celdaKey = celdaKey;
	}

	public Double getRend() {
		return rend;
	}

	public void setRend(Double rend) {
		this.rend = rend;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		celdaKey.readFields(in);
		rend = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		celdaKey.write(out);
		out.writeDouble(rend);
	}

	@Override
	public String toString() {
		return celdaKey.toString() + "	" + rend;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((celdaKey == null) ? 0 : celdaKey.hashCode());
		result = prime * result + ((rend == null) ? 0 : rend.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CeldaValueWritable other = (CeldaValueWritable) obj;
		if (celdaKey == null) {
			if (other.celdaKey != null)
				return false;
		} else if (!celdaKey.equals(other.celdaKey))
			return false;
		if (rend == null) {
			if (other.rend != null)
				return false;
		} else if (!rend.equals(other.rend))
			return false;
		return true;
	}

	@Override
	public int compareTo(CeldaValueWritable cw) {
		int cmp = celdaKey.compareTo(cw.getCeldaKey());
		if (cmp != 0) {
			return cmp;
		}
		return rend.compareTo(cw.getRend()) * (-1);
	}

}
