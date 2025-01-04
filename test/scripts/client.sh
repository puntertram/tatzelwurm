# This scripts simulates lot of clients calling read_file for the same file
cd /home/puneeth/Documents/projects/tatzelwurm
start_time=$(date +%s)
# sleep 3
for i in {31..31}
do
   python3 src/service/client/python/client.py config/client/client_config.json $i > test/output/client_$i.out 2>&1 &
    # python3 client.py /home/puneeth/Documents/projects/tatzelwurm/client_config.json > /dev/null &
done
wait
end_time=$(date +%s)
echo $end_time
echo $start_time
elapsed_time=$((end_time - start_time))  # Calculate the difference
echo "Elapsed time: $elapsed_time seconds"