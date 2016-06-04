import os, shutil, sys
import urllib2, traceback
import pysam
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Kling").setMaster("spark://130.238.28.106:7077")
conf.set("spark.executor.memory", "5000m")
conf.set("spark.executor.cores","2")

sc = SparkContext(conf=conf)

def pysamParseBam(bam):
    reads = []
    readPositions = []

    try:
        pysamBam = pysam.AlignmentFile(bam, "rb")
        for i in pysamBam.fetch(until_eof=True):
            if i.is_unmapped:
                readPositions.append(str(i.reference_start))
                reads.append(i.query_alignment_sequence)
        pysamBam.close()
    except:
        print "Exit with error"
        return (bam, (reads, readPositions))
    else:
        print "Exit with success"
        return (bam, (reads, readPositions))

def extractKmersFromRead(reads):
    kmers = {}
    for read in reads:
        for i in range(0, len(read)-9):
            kmer = read[i:i+10]
            if not kmers.has_key(kmer):
                kmers[kmer] = 1
    return kmers.items()

def extractBamfileNames():
    bamFiles = []
    if not os.path.exists("bams.html"):
        with open("bams.html", "w") as f:
            f.write(urllib2.urlopen("http://130.238.29.253:8080/swift/v1/1000-genomes-dataset/").read())
    with open("bams.html", "r") as f:
        for fileName in f:
            fileName = fileName.rstrip("\n")
            if fileName[-3:] == "bam":
                bamFiles.append(fileName)
    return bamFiles

def main():
    bamFiles = extractBamfileNames()
    bamFiles = bamFiles[:2]
    print len(bamFiles)
    distFiles = sc.parallelize(bamFiles)
    print "##############################################################"
    result = distFiles.flatMap(lambda file: pysamParseBam(file))
    print "##############################################################"
    kmers = result.flatMap(lambda tuple: createKmers(tuple[0])).reduceByKey(lambda a,b: a+b).filter(lambda kmer: kmer[1] >= 10 and kmer[1] <= 200).sortBy(lambda k: k[1])
    print "##############################################################"
    with open("kmers.txt", "a") as f:
        f.write(str(kmers_res))

main()
