# Tatzelwurm

<img src="tatzelwurm.jpeg" width="300">

# Setup
1. Make the setup script in test/scripts/setup.sh executable by running the command: `chmod +x test/scripts/setup.sh`
2. Run the setup script and pass the absolute path of the directory where you want to setup the meta files(like the WAL audit log, data store files, etc.), for example `./setup.sh ~/Documents/projects/tatzelwurm`

# Local deployment
1. Start the mainserver from the src directory(please cd into that directory if not already done :)) : `go run main.go --run_mode mainserver --config_path  <absolute path to the mainserver_config>` Example config files are given in `config/mainserver` folder
2. Start the chunkservers: `go run main.go --run_mode chunkserver --config_path <absolute path to the chunkserver_config>` Example config files are given in `config/chunkserver` folder 
3. Start the example client: `python3 client.py <absolute path to the client_config` Example config files are given in `config/client` folder
