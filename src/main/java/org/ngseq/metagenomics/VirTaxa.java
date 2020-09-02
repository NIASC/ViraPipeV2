package org.ngseq.metagenomics;

import org.apache.spark.annotation.Private;

import java.io.Serializable;

public class VirTaxa implements Serializable {


    private String acc;
    private String taxa;


    public String getAcc() {
        return acc;
    }

    public void setAcc(String acc) {
        this.acc = acc;
    }

    public String getTaxa() {return taxa;}

    public void setTaxa(String taxa) {this.taxa= taxa;}




}
