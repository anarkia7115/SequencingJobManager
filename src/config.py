import os
host = dict(
     hdfshost           = "node19"
    ,hdfs               = "node19:9000"
    ,rmhost             = "node19"
    ,request            = "mgmt:8081"
    ,local_server_host  = "node19"
    ,local_server_port  = 8321
)

jar = dict(
     upload = "/online/home/GCBI/jars/halvade_upload-1.0-no_local-jar-with-dependencies.jar"
    ,align  = "/online/home/GCBI/jars/align_filter-wxz-1.0.jar"
    ,snv    = "/online/home/GCBI/jars/snv_job-wxz-1.4.jar"
    #,snv    = "/online/home/GCBI/jars/snv_job_first_phase.jar"
    #,merge  = "/online/home/GCBI/jars/snv_job_second_phase.jar"
)

hdfs_base = dict(
     upload     = "/user/GCBI/sequencing/fastq_{0}"
    ,align      = "/user/GCBI/sequencing/align_{0}"
    ,snv        = "/user/GCBI/sequencing/snv_{0}"
    ,qa         = "/user/GCBI/sequencing/qa_{0}"
    ,pkgResult  = "/user/GCBI/sequencing/pkg_{0}"
)

hdfs_out = dict(
     upload = "hdfs://{0}{1}".format(host['hdfs'], hdfs_base['upload'])
    ,align  = "hdfs://{0}{1}".format(host['hdfs'], hdfs_base['align'])
    ,snv    = "hdfs://{0}{1}".format(host['hdfs'], hdfs_base['snv'])
    ,vcf    = "hdfs://{0}{1}".format(host['hdfs'], hdfs_base['snv']) + \
                                           "/merge/HalvadeCombined.vcf"
    ,qa     = "hdfs://{0}{1}".format(host['hdfs'], hdfs_base['qa'])
)

hdfs_in = dict(
     align  = hdfs_out['upload']
    ,qa     = hdfs_out['upload']
    ,snv    = hdfs_out['align'] + "/bamfiles"
)

hdfs_config = dict(
    empty_vcf   = "hdfs://{0}/tmp/dbsnp_138.hg38.vcf".format(host['hdfs'])
    ,vcf_gatk   = "hdfs://{0}/ref/hg38/dbsnp/dbsnp_146.hg38.vcf.gz".format(host['hdfs'])
    ,bin        = "hdfs://{0}/user/GCBI/bin.tar.gz".format(host['hdfs'])
    ,ref        = "hdfs://{0}/ref/hg38/Homo_sapiens_assembly38".format(host['hdfs'])
    ,signal     = "hdfs://{0}/signal/sequencing_{1}".format(host['hdfs'], "{0}")
)

local_config = dict(
     tmp_folder       = "/gcbi/halvade"
    ,tmp_manifest     = "/tmp/sample_{0}.manifest"
    ,local_fastq      = "/online/GCBI/fastq/{0}"
    ,local_qa         = "/online/GCBI/qa/{0}"
    ,local_pkgResult  = "/online/GCBI/pkgResult/{0}"


    ,local_vcf_header = "/online/home/GCBI/git/combine-file/vcf.header"

    ,local_vcf        = "/online/GCBI/pkgResult/{0}/HalvadeCombined.vcf"
    ,local_snp        = "/online/GCBI/pkgResult/{0}/HalvadeCombined.vcfresult.snp"
    ,local_indel      = "/online/GCBI/pkgResult/{0}/HalvadeCombined.vcfresult.indel"

    ,local_snp_out    = "/online/GCBI/pkgResult/{0}/sample_basic_snp-snp.vcf"
    ,local_indel_out  = "/online/GCBI/pkgResult/{0}/sample_basic_indel-indel.vcf"
    ,local_indel_out2 = "/online/GCBI/pkgResult/{0}/sample_basic2_indel-indel.vcf"
    ,local_result     = "/online/GCBI/result/{0}"
)

bin = dict(
     qa                 = "/online/home/GCBI/git/qc_for_ngs/bin/qc"
    ,combine_snp_indel  = "/online/home/GCBI/git/combine-file/combine-snp-indel.sh"
    ,separ_snp_indel    = "/online/home/GCBI/git/combine-file/separ_snp_indel"
    #,vcf4convert       = "/online/home/GCBI/git/combine-file/vcf4convert"
    ,vcf4convert        = "touch"
)
