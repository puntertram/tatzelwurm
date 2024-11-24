# This scripts simulates lot of clients calling read_file for the same file
for i in {1..2}
do
   python3 client.py /Users/puneeth/Documents/software/side-projects/gfs-clone/client_config.json $i > testing/output/client_$i.out 2>&1 &
    # python3 client.py /Users/puneeth/Documents/software/side-projects/gfs-clone/client_config.json > /dev/null &
done