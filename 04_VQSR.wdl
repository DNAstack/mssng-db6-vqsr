workflow mergeMain {
    Array [File] main_vcfs
    Array [File] main_vcf_indices
    Array [File] bed_files
    String joint_samplename
    
    # Known sites
    File dbsnp_vcf
    File dbsnp_index
    File ref_alt
    File ref_fasta
    File ref_fasta_index
    File ref_dict
    File ref_bwt
    File ref_sa
    File ref_amb
    File ref_ann
    File ref_pac
    File mills_vcf
    File mills_vcf_index
    File hapmap_vcf
    File hapmap_vcf_index
    File omni_vcf
    File omni_vcf_index
    File onekg_vcf
    File onekg_vcf_index
    File axiom_poly_vcf
    File axiom_poly_vcf_index

    # Sentieon License configuration
    File? sentieon_license_file
    String sentieon_license_server = ""
    Boolean use_instance_metadata = false
    String? sentieon_auth_mech
    String? sentieon_license_key

    # Execution configuration
    String threads = "8"
    String memory = "30 GB"
    String sentieon_version = "201808.06"
    String docker = "dnastack/sentieon-bcftools:${sentieon_version}"


    call mergeVCFs {
        input:
            main_vcfs = main_vcfs,
            main_vcf_indices = main_vcf_indices,
            joint_samplename = joint_samplename,
            # Execution configuration
            docker = docker,
            threads = threads,
            memory = memory
    }
    
    call VQSR {
        input:
            mergedVCF = mergeVCFs.mergedVCF,
            joint_samplename = joint_samplename,
            # Known sites
            dbsnp_vcf = dbsnp_vcf,
            dbsnp_index = dbsnp_index,
            mills_vcf = mills_vcf,
            mills_vcf_index = mills_vcf_index,
            hapmap_vcf = hapmap_vcf,
            hapmap_vcf_index = hapmap_vcf_index,
            omni_vcf = omni_vcf,
            omni_vcf_index = omni_vcf_index,
            onekg_vcf = onekg_vcf,
            onekg_vcf_index = onekg_vcf_index,
            axiom_poly_vcf = axiom_poly_vcf,
            axiom_poly_vcf_index = axiom_poly_vcf_index,
            # Reference files
            ref_fasta = ref_fasta,
            ref_fasta_index = ref_fasta_index,
            ref_dict = ref_dict,
            ref_alt = ref_alt,
            ref_bwt = ref_bwt,
            ref_sa = ref_sa,
            ref_amb = ref_amb,
            ref_ann = ref_ann,
            ref_pac = ref_pac,
            # Sentieon License configuration
            sentieon_license_server = sentieon_license_server,
            sentieon_license_file = sentieon_license_file,
            use_instance_metadata = use_instance_metadata,
            sentieon_auth_mech = sentieon_auth_mech,
            sentieon_license_key = sentieon_license_key,
            # Execution configuration
            docker = docker
    }

    scatter (region in bed_files) {
        call separateVQSR_VCF {
            input:
                vqsr_VCF = VQSR.vqsr_VCF,
                vqsr_VCF_index = VQSR.vqsr_VCF_index,
                region = region,
                joint_samplename = joint_samplename,
                # Sentieon License configuration
                sentieon_license_server = sentieon_license_server,
                sentieon_license_file = sentieon_license_file,
                use_instance_metadata = use_instance_metadata,
                sentieon_auth_mech = sentieon_auth_mech,
                sentieon_license_key = sentieon_license_key,
                # Execution configuration
                threads = threads,
                memory = memory,
                docker = docker
        }
    }

    output {
        Array [File] vqsr_main_recal_vcfs = separateVQSR_VCF.recal_vcf
        Array [File] vqsr_main__recal_indices = separateVQSR_VCF.recal_vcf_index
        File recal_vcf_full = VQSR.vqsr_VCF
        File recal_vcf_full_index = VQSR.vqsr_VCF_index
        File vqsr_plot = VQSR.vqsr_plot
    }

    meta {
    author: "Heather Ward"
    email: "heather@dnastack.com"
    description: "## MSSNG DB6 VQSR\n\nPerform VQSR on the `GVCFtyper_main` files - they will be combined into a single file, VQSR performed, and then re-split by chromosome. All chromosome `GVCFtyper_main` files should be input here, and one `GVCFtyper_main.recal` recalibrated main file per chromosome will be output. Also input the `chr__.bed` region files from the previous step.\n\n#### Running Sentieon\n\nIn order to use Sentieon, you must possess a license, distributed as either a key, a server, or a gcp project. The license may be attained by contacting Sentieon, and must be passed as an input to this workflow."
  }
}

task mergeVCFs {
    Array [File] main_vcfs
    Array [File] main_vcf_indices
    String joint_samplename

    String threads
    String memory
    String docker
    Int disk_size = ceil(size(main_vcfs[0], "GB")*length(main_vcfs) + 100)

    command {
        bcftools concat \
            ${sep=' ' main_vcfs} \
            -O z \
            -o ${joint_samplename}_GVCFtyper_main.vcf.gz
    }

    output {
        File mergedVCF = "${joint_samplename}_GVCFtyper_main.vcf.gz"
    }

    runtime {
        docker: docker
        cpu: threads
        memory: memory
        disks: "local-disk " + disk_size + " HDD"
    }
}    

task VQSR {
    File mergedVCF
    String joint_samplename

    # Known sites
    File? dbsnp_vcf
    File? dbsnp_index

    File mills_vcf
    File mills_vcf_index
    File hapmap_vcf
    File hapmap_vcf_index
    File omni_vcf
    File omni_vcf_index
    File onekg_vcf
    File onekg_vcf_index
    File axiom_poly_vcf
    File axiom_poly_vcf_index

    # Reference files
    File ref_fasta
    File ref_fasta_index
    File ref_dict
    File ref_alt
    File ref_bwt
    File ref_sa
    File ref_amb
    File ref_ann
    File ref_pac

    # Sentieon License configuration
    File? sentieon_license_file
    String sentieon_license_server
    Boolean use_instance_metadata
    String? sentieon_auth_mech
    String? sentieon_license_key

    # Execution configuration
    String docker

    command {
        set -exo pipefail
        mkdir -p /tmp
        export TMPDIR=/tmp

        # License server setup
        license_file=${default="" sentieon_license_file}
        if [[ -n "$license_file" ]]; then
          # Using a license file
          export SENTIEON_LICENSE=${default="" sentieon_license_file}
        elif [[ -n '${true="yes" false="" use_instance_metadata}' ]]; then
          python /opt/sentieon/gen_credentials.py ~/credentials.json ${default="''" sentieon_license_key} &
          sleep 5
          export SENTIEON_LICENSE=${default="" sentieon_license_server}
          export SENTIEON_AUTH_MECH=${default="" sentieon_auth_mech}
          export SENTIEON_AUTH_DATA=~/credentials.json
          read -r SENTIEON_JOB_TAG < ~/credentials.json.project
          export SENTIEON_JOB_TAG
        else
          export SENTIEON_LICENSE=${default="" sentieon_license_server}
          export SENTIEON_AUTH_MECH=${default="" sentieon_auth_mech}
        fi

        # Optimizations
        export MALLOC_CONF=lg_dirty_mult:-1


        sentieon util vcfindex ${mergedVCF}

        # VQSR
        resource_text_SNP="--resource ${hapmap_vcf} --resource_param HapMap,known=false,training=true,truth=true,prior=15.0 "
        resource_text_SNP="$resource_text_SNP --resource ${omni_vcf} --resource_param Omni,known=false,training=true,truth=true,prior=12.0 "
        resource_text_SNP="$resource_text_SNP --resource ${onekg_vcf} --resource_param 1000G,known=false,training=true,truth=false,prior=10.0 "
        resource_text_SNP="$resource_text_SNP --resource ${dbsnp_vcf} --resource_param dbSNP,known=true,training=false,truth=false,prior=2.0"

        resource_text_indel="--resource ${mills_vcf} --resource_param Mills,known=false,training=true,truth=true,prior=12.0 "
        resource_text_indel="$resource_text_indel --resource ${dbsnp_vcf} --resource_param dbSNP,known=true,training=false,truth=false,prior=2.0"
        resource_text_indel="$resource_text_indel --resource ${axiom_poly_vcf} --resource_param axiomPoly,known=false,training=true,truth=false,prior=10.0"
          
        #SNP RECAL
        sentieon driver -t 64 -r ${ref_fasta} --algo VarCal -v ${mergedVCF} --max_gaussians 6 --tranches_file ${joint_samplename}.SNP.tranches  --var_type SNP --plot_file ${joint_samplename}.SNP.varcal.plot $resource_text_SNP --annotation QD --annotation MQ --annotation MQRankSum --annotation ReadPosRankSum --annotation FS --tranche 100 --tranche 99.95 --tranche 99.9 --tranche 99.8 --tranche 99.6 --tranche 99.5 --tranche 99.4 --tranche 99.3 --tranche 99.0 --tranche 98.0 --tranche 97.0 --tranche 90.0 ${joint_samplename}_GVCFtyper_main.vcf.SNP.recal

        sentieon driver -t 64 -r ${ref_fasta} --algo ApplyVarCal --sensitivity 99.7 -v ${mergedVCF} --var_type SNP --recal ${joint_samplename}_GVCFtyper_main.vcf.SNP.recal --tranches_file ${joint_samplename}.SNP.tranches ${joint_samplename}_GVCFtyper_main.vcf.SNP.recaled.vcf.gz


        #INDEL RECAL
        sentieon driver -t 64 -r ${ref_fasta} --algo VarCal -v ${joint_samplename}_GVCFtyper_main.vcf.SNP.recaled.vcf.gz $resource_text_indel --max_gaussians 4 --annotation QD --annotation ReadPosRankSum --annotation FS --tranche 100.0 --tranche 99.95 --tranche 99.9 --tranche 99.5 --tranche 99.0 --tranche 97.0 --tranche 96.0 --tranche 95.0 --tranche 94.0 --tranche 93.5 --tranche 93.0 --tranche 92.0 --tranche 91.0 --tranche 90.0 --var_type INDEL --tranches_file ${joint_samplename}.INDEL.tranches ${joint_samplename}_GVCFtyper_main.vcf.SNP.INDEL.recal

        sentieon driver -t 64 -r ${ref_fasta} --algo ApplyVarCal -v ${joint_samplename}_GVCFtyper_main.vcf.SNP.recaled.vcf.gz --sensitivity 99.7 --var_type INDEL --recal ${joint_samplename}_GVCFtyper_main.vcf.SNP.INDEL.recal  --tranches_file ${joint_samplename}.INDEL.tranches ${joint_samplename}_GVCFtyper_main.recal.vcf.gz

          
        sentieon plot vqsr -o ${joint_samplename}.SNP.VQSR.pdf ${joint_samplename}.SNP.varcal.plot 
    }

    output {
        File vqsr_VCF = "${joint_samplename}_GVCFtyper_main.recal.vcf.gz"
        File vqsr_VCF_index = "${joint_samplename}_GVCFtyper_main.recal.vcf.gz.tbi"
        File vqsr_plot = "${joint_samplename}.SNP.VQSR.pdf"
    }

    runtime {
        docker: docker
        cpu: 55
        memory: "240 GB"
        disks: "local-disk 400 LOCAL"
    }
}

task separateVQSR_VCF {
    File vqsr_VCF
    File vqsr_VCF_index
    File region
    String joint_samplename
    String chromosome = basename(region)

    # Sentieon License configuration
    File? sentieon_license_file
    String sentieon_license_server
    Boolean use_instance_metadata
    String? sentieon_auth_mech
    String? sentieon_license_key

    Int disk_size = ceil(size(vqsr_VCF, "GB")*4 + 50)
    String docker
    String threads
    String memory


    command {
        set -exo pipefail
        mkdir -p /tmp
        export TMPDIR=/tmp

        # License server setup
        license_file=${default="" sentieon_license_file}
        if [[ -n "$license_file" ]]; then
          # Using a license file
          export SENTIEON_LICENSE=${default="" sentieon_license_file}
        elif [[ -n '${true="yes" false="" use_instance_metadata}' ]]; then
          python /opt/sentieon/gen_credentials.py ~/credentials.json ${default="''" sentieon_license_key} &
          sleep 5
          export SENTIEON_LICENSE=${default="" sentieon_license_server}
          export SENTIEON_AUTH_MECH=${default="" sentieon_auth_mech}
          export SENTIEON_AUTH_DATA=~/credentials.json
          read -r SENTIEON_JOB_TAG < ~/credentials.json.project
          export SENTIEON_JOB_TAG
        else
          export SENTIEON_LICENSE=${default="" sentieon_license_server}
          export SENTIEON_AUTH_MECH=${default="" sentieon_auth_mech}
        fi

        # Optimizations
        export MALLOC_CONF=lg_dirty_mult:-1        

        bcftools view \
            -R ${region} \
            -O z \
            -o ${joint_samplename}_GVCFtyper_main_${chromosome}.recal.vcf.gz \
            ${vqsr_VCF}

        sentieon util vcfindex ${joint_samplename}_GVCFtyper_main_${chromosome}.recal.vcf.gz
    }

    output {
        File recal_vcf = "${joint_samplename}_GVCFtyper_main_${chromosome}.recal.vcf.gz"
        File recal_vcf_index = "${joint_samplename}_GVCFtyper_main_${chromosome}.recal.vcf.gz.tbi" 
    }

    runtime {
        docker: docker
        cpu: threads
        memory: memory
        disks: "local-disk " + disk_size + " HDD"
        preemptible: 2
    }

}