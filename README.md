ViraPipe
==============

ViraPipe is a Apache Spark based scalable parallel pipeline for analysing metagenomes from NGS read data in a computing cluster or on a multi-core standalone machine.
The pipeline is designed especially for viral metagenomes but the software is applicable for any other genome analysis purposes.
The pipeline integrates parallel BWA-MEM read aligner, MegaHit DeNovo assembler, BLAST and HMMSearch tools. Pipeline includes
also tools for sequence data normalization and filtration as well as SQL query interfaces for querying the data in parallel.
FASTQ, FASTA, SAM/BAM and Apache Parquet formats are supported as well as BLAST tabular format.

------------------------------------------------------------------------------------------
Requirements
------------------------------------------------------------------------------------------

Apache Spark 2 installed with Hadoop, YARN and HDFS filesystem.
Tested on Ubuntu 16 and Centos 7 servers.
Java 1.8

Dependencies:
Hadoop-BAM 7.4.0 +
Samtools HtsJDK 2.5.0 +
Java Bindings (JNI) for bwa: jbwa 1.0.0 +

Scala 2.10 +
Scala parsing library Scallop 2.1.2 +

------------------------------------------------------------------------------------------
Installation
------------------------------------------------------------------------------------------
### Compiling ViraPipe

    git clone https://github.com/NGSeq/ViraPipe
    cd ViraPipe
    mvn install package

### jbwa
    git clone https://github.com/lindenb/jbwa
    cd jbwa
    make

    Copy libbwajni native library to {$HADOOP_HOME}/lib/native directory on every worker node e.g. with scp:
    scp src/main/native/libbwajni.so username@hostname:/{$HADOOP_HOME}/lib/native

### Download and install MegaHit assembler on every worker node (requires gcc compiler)
    git clone https://github.com/voutcn/megahit
    cd megahit
    make
    create symbolic link
        ln -s {path to megahit}/megahit /usr/local/bin/megahit
    or add megahit to PATH

### Download and install BLAST and HMMER software on every worker node (requires gcc compiler)

[HMMER](http://www.hmmer.org/download.html)
    wget http://eddylab.org/software/hmmer3/3.1b2/hmmer-3.1b2-linux-intel-x86_64.tar.gz
    Extract and make

[BLAST](https://blast.ncbi.nlm.nih.gov/Blast.cgi?PAGE_TYPE=BlastDocs&DOC_TYPE=Download)
    wget ftp://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/LATEST/ncbi-blast-2.6.0+-x64-linux.tar.gz
    Extract and make

### Download and extract BLAST and HMMER databases under the same path on every worker node
##### vFam database for hmmsearch
    wget http://derisilab.ucsf.edu/software/vFam/vFam-B_2014.hmm
##### BLAST database files
    for i in {0..9}; do wget ftp://ftp.ncbi.nlm.nih.gov/blast/db/nt.0$i.tar.gz ; done
    for i in {10..50}; do wget ftp://ftp.ncbi.nlm.nih.gov/blast/db/nt.$i.tar.gz ; done
    for i in {0..9}; do wget ftp://ftp.ncbi.nlm.nih.gov/blast/db/human_genomic.0$i.tar.gz ; done
    for i in {10..22}; do wget ftp://ftp.ncbi.nlm.nih.gov/blast/db/human_genomic.$i.tar.gz ; done
    wget ftp://ftp.ncbi.nlm.nih.gov/blast/db/taxdb.tar.gz
    cat *.gz | tar -xzvf - -i
    
##### Copy DBs to every node with scp
    scp vFam-B_2014.hmm username@hostname:/database/hmmer
    scp nt.* username@hostname:/database/blast/nt
    scp human_genomic.* username@hostname:/database/blast/hg
    scp taxdb.* username@hostname:/database/taxdb

##### Set BLASTDB environment variable on each node:
    export BLASTDB=$BLASTDB:/database/blast/nt
    export BLASTDB=$BLASTDB:/database/blast/hg
    export BLASTDB=$BLASTDB:/database/taxdb
------------------------------------------------------------------------------------------
Running the example pipeline
------------------------------------------------------------------------------------------

### Download human reference genome index on every node under the same path e.g. /index
    wget -r ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/reference/GRCh38_reference_genome/*

### Download NGS sequence files and load to HDFS
    wget ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/data/HG00313/sequence_read/ERR016234_1.filt.fastq.gz
    wget ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/data/HG00313/sequence_read/ERR016234_2.filt.fastq.gz
    hdfs dfs -mkdir /data/input/example
    hdfs dfs -mkdir /data/output
    hdfs dfs -put ERR016234_1.filt.fastq.gz /data/input/example
    hdfs dfs -put ERR016234_2.filt.fastq.gz /data/input/example

### Run the pipeline
    Check that configuration of Spark master, num-executors, executor-memory etc. fit your system and that classpath, directories, databases etc. exists and user has proper permissions.
    scripts/virapipe.sh /data/input /data/output example
    
    
------------------------------------------------------------------------------------------
Citation
------------------------------------------------------------------------------------------
AI Maarala, Z Bzhalava, J Dillner, K Heljanko, D Bzhalava;
ViraPipe: Scalable Parallel Pipeline for Viral Metagenome Analysis from Next Generation Sequencing Reads, Bioinformatics, Nov. 2017, btx702, https://doi.org/10.1093/bioinformatics/btx702
