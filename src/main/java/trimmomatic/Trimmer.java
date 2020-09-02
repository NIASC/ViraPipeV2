package trimmomatic;

public interface Trimmer
{
	public FastqRecord[] processRecords(FastqRecord in[]);
}
