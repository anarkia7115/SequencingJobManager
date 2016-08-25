import os
host = dict(
    hdfs = "localhost:9000",
    rmhost = "localhost",
    request = "192.168.2.156:8081",
    local_server_host = "192.168.2.156",
    local_server_port = 8080
)

jar = dict(
    upload = "../lib/halvade_upload-1.0-no_local-jar-with-dependencies.jar",
    align = "../lib/align_filter-wxz-1.0.jar",
    snv = "/jars/snv_job-wxz-1.2.jar"
)

hdfs_out = dict(
    upload = "hdfs://{0}/user/GCBI/sequencing/fastq_{1}".format(host['hdfs'], "{0}"),
    align = "hdfs://{0}/user/GCBI/sequencing/align_{1}".format(host['hdfs'], "{0}"),
    snv = "hdfs://{0}/user/GCBI/sequencing/snv_{1}".format(host['hdfs'], "{0}")
    qa = "hdfs://{0}/user/GCBI/sequencing/qa_{1}".format(host['hdfs'], "{0}")
)

hdfs_in = dict(
    align = hdfs_out['upload']
    snv = os.path.join(hdfs_out['align'], "bamfiles")
    qa = hdfs_out['upload']
)

hdfs_config = dict(
    empty_vcf = "hdfs://{0}:9000/tmp/dbsnp_138.hg38.vcf".format(host['hdfs']),
    bin = "hdfs://{0}:9000/user/GCBI/bin.tar.gz".format(host['hdfs']),
    ref = "hdfs://{0}:9000/ref/hg38/hg38".format(host['hdfs']),
    signal = "hdfs://{0}:9000/signal/sequencing_{1}".format(host['hdfs'], "{0}")
)

local_config = dict(
    tmp_folder = "/tmp/halvade",
    tmp_manifest = "/tmp/sample_{0}.manifest", 
    local_fastq = "/online/GCBI/fastq/{0}"
    local_qa = "/online/GCBI/qa/{0}"
)

bin = dict(
    qa = "/online/home/GCBI/git/qc_for_ngs/bin", 
    pkgResult = "/online/home/GCBI/workspace/combine-file/combine-snp-indel.sh"
)
