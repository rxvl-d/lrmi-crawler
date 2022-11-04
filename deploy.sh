sbt docker:publishLocal
docker tag lrmi-crawler:0.1.0-SNAPSHOT rjsvaljean/lrmi-crawler:$1
docker push rjsvaljean/lrmi-crawler:$1
ssh devbox2 -- rm -f *.sif
ssh devbox2 -- mkdir lrmi-crawler-out-cc lrmi-crawler-out-wdc19 lrmi-crawler-out-wdc20 lrmi-crawler-out-wdc21
ssh devbox2 -- singularity pull docker://rjsvaljean/lrmi-crawler:$1
cat run_sbatch.sh.template | sed "s/SIF_FILE/lrmi-crawler_$1.sif/" | sed 's/N_CORES/40/' | sed 's/SOURCE/wdc19/' > run_sbatch.sh
scp run_sbatch.sh devbox2:run_sbatch_19.sh
rm run_sbatch.sh
cat run_sbatch.sh.template | sed "s/SIF_FILE/lrmi-crawler_$1.sif/" | sed 's/N_CORES/40/' | sed 's/SOURCE/wdc20/' > run_sbatch.sh
scp run_sbatch.sh devbox2:run_sbatch_20.sh
rm run_sbatch.sh
cat run_sbatch.sh.template | sed "s/SIF_FILE/lrmi-crawler_$1.sif/" | sed 's/N_CORES/40/' | sed 's/SOURCE/wdc21/' > run_sbatch.sh
scp run_sbatch.sh devbox2:run_sbatch_21.sh
rm run_sbatch.sh
ssh devbox2 -- sbatch -c 10 -J lrmi19 -o output_19.log -e err_19.log run_sbatch_19.sh
ssh devbox2 -- sbatch -c 10 -J lrmi20 -o output_20.log -e err_20.log run_sbatch_20.sh
ssh devbox2 -- sbatch -c 10 -J lrmi21 -o output_21.log -e err_21.log run_sbatch_21.sh
