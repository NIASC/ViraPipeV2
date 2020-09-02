package trimmomatic;

public class LeadingTrimmer extends AbstractSingleRecordTrimmer
{
    private int qual;


    public LeadingTrimmer(int qual) {
        this.qual = qual;
    }
        
        

    @Override
    public FastqRecord processRecord(FastqRecord in)
    {
            String seq=in.getSequence();
            int quals[]=in.getQualityAsInteger(true);
            for(int i=0;i<seq.length();i++)
                    {
                    if(quals[i]>=qual)
                            return new FastqRecord(in,i,seq.length()-i);
                    }

            return null;
    }

}
