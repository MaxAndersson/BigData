import os, shutil, sys, pysam, urllib2
import plotly.plotly as py
import plotly.graph_objs as go
import plotly.tools as pl
from pyspark import SparkContext, SparkConf

#Global variables
NR_OF_FILES = 20 #Number of bam files to be computed

conf = SparkConf().setAppName("Kling").setMaster("spark://130.238.28.106:7077")

sc = SparkContext(conf=conf)
pl.set_credentials_file(username='', api_key='') # Insert to use plotly

def pysamParseBam(bam):
    reads = []
    readPositions = []

    try:
        pysamBam = pysam.AlignmentFile("http://130.238.29.253:8080/swift/v1/1000-genomes-dataset/" + bam, "rb")
        for i in pysamBam.fetch(until_eof=True):
            if i.is_unmapped:
                readPositions.append(str(i.reference_start))
                reads.append(i.query_alignment_sequence)
        pysamBam.close()
    except:
        if os.path.exists(bam + ".bai"):
            os.remove(bam + ".bai")
        kmers = extractKmersFromReads(reads)
        return (kmers, readPositions)
    else:
        if os.path.exists(bam + ".bai"):
            os.remove(bam + ".bai")
        kmers = extractKmersFromReads(reads)
        return (kmers, readPositions)

def extractKmersFromReads(reads):
    kmers = []
    for read in reads:
        for i in range(0, len(read)-9):
            kmer = read[i:i+10]
            kmers.append((kmer,1))
    return list(set(kmers))

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

def isInt(x):
    val = 0
    try:
        val = int(x)
    except ValueError:
        pass
    return val

def main():
    bamFiles = extractBamfileNames()
    bamFiles = bamFiles[:NR_OF_FILES]

    files = sc.parallelize(bamFiles, len(bamFiles))

    kmersAndPositions = files.map(lambda bam: pysamParseBam(bam))

    kmers = kmersAndPositions.flatMap(lambda kp: kp[0]).reduceByKey(lambda a,b: a+b).filter(lambda kmer: kmer[1] >= 10 and kmer[1] <= 200).sortBy(lambda k: k[1])
    positions = kmersAndPositions.flatMap(lambda kp: kp[1]).map(lambda pos: (isInt(pos) - (isInt(pos) % 10000),1)).reduceByKey(lambda a,b: a+b).sortBy(lambda f: f[0])

    for kmer in kmers.collect():
        with open("kmers.txt", "a") as f:
            f.write(str(kmer) + "\n")

    posX = []
    posY = []

    for pos in positions.collect():
        posX.append(pos[0])
        posY.append(pos[1])

    trace = go.Scatter(x = posX, y = posY, mode = 'markers')
    data = [trace]
    plot_url = py.plot(data, filename='positions')

main()
