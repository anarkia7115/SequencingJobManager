import os
host = dict(
    hdfshost = "node19",
    hdfs = "node19:9000",
    rmhost = "node19",
    request = "mgmt:8081",
    local_server_host = "node19",
    local_server_port = 8321
)

jar = dict(
    upload = "/online/home/GCBI/jars/halvade_upload-1.0-no_local-jar-with-dependencies.jar", 
    align = "/online/home/GCBI/jars/align_filter-wxz-1.0.jar",
    snv = "/online/home/GCBI/jars/snv_job-wxz-1.2.jar"
)

hdfs_base = dict(
    upload = "/user/GCBI/sequencing/fastq_{0}",
    align = "/user/GCBI/sequencing/align_{0}",
    snv = "/user/GCBI/sequencing/snv_{0}",
    qa = "/user/GCBI/sequencing/qa_{0}",
    pkgResult = "/user/GCBI/sequencing/pkg_{0}"
)

hdfs_out = dict(
    upload = "hdfs://{0}{1}".format(host['hdfs'], hdfs_base['upload']),
    align = "hdfs://{0}{1}".format(host['hdfs'], hdfs_base['align']),
    snv = "hdfs://{0}{1}".format(host['hdfs'], hdfs_base['snv']),
    qa = "hdfs://{0}{1}".format(host['hdfs'], hdfs_base['qa'])
)

hdfs_in = dict(
    align = hdfs_out['upload'],
    snv = os.path.join(hdfs_out['align'], "bamfiles"),
    qa = hdfs_out['upload'],
    pkgResult = "hdfs://{0}{1}".format(host['hdfs'], hdfs_base['pkgResult'])
)

hdfs_config = dict(
    empty_vcf = "hdfs://{0}/tmp/dbsnp_138.hg38.vcf".format(host['hdfs']),
    bin = "hdfs://{0}/user/GCBI/bin.tar.gz".format(host['hdfs']),
    ref = "hdfs://{0}/ref/hg38/hg38".format(host['hdfs']),
    signal = "hdfs://{0}/signal/sequencing_{1}".format(host['hdfs'], "{0}")
)

local_config = dict(
    tmp_folder = "/tmp/halvade",
    tmp_manifest = "/tmp/sample_{0}.manifest", 
    local_fastq = "/online/GCBI/fastq/{0}", 
    local_qa = "/online/GCBI/qa/{0}"
)

bin = dict(
    qa = "/online/home/GCBI/git/qc_for_ngs/bin/qc", 
    pkgResult = "/online/home/GCBI/workspace/combine-file/combine-snp-indel.sh"
)
