# Matrix

An agent based modeling framework for social simulation.

## Installation Instructions

It is recommended that you install this package within a virtual environment
created with either virtualenv or conda.

### Creating and activating a conda environment

To create a new virtual environment with conda,
have Anaconda setup on your system.
Installation instructions for Anaconda can be found at:
https://conda.io/docs/user-guide/install/index.html
After installation of Anaconda execute the following commands.

$ conda create -n matrixenv python=3
$ source activate matrixenv

### Creating and activating a virtualenv enviroment

To create a new virtual environment with virtualenv
install virtualenv using your system's package manager.
After installation of virtualenv execute the following commands.

$ virtualenv -p python3 matrixenv
$ . matrxienv/bin/activate

### Install matrix

Copy the current version of the matrix source in the current directory,
and execute the following command.

$ pip install ./Matrix-<Version>.tar.gz

The above should make the matrix command available.
You can check if installation was successful with the following command.

$ matrix --help

## Testing Matrix: Simple Setup - one controller and two simple agents

Start three separate terminal windows and activate the conda or virtualenv
environment in all of them.

### Step 1: Create the initial events database

In the first terminal window, execute the following command

$ matrix initdb --event-db ./event.db --num-agents 2 --num-repos 2

The above command will create an events database representing global state,
where there are two Github repositories, created by two agents.

NOTE: in the current version the number of agents and number of repositories
needs to be the same.

### Step 2: Start the controller process

Execute the following command in the first terminal window.

$ matrix controller --address 127.0.0.1:16001 --event-db ./event.db --num-agents 2 --num-rounds 10

The above command will start a controller process that listens on
port 16001 for messages from agents, and writes out events
to the events.db file. Also the controller knows that there are two agents
in the current simulation and that the simulation will run for 10 rounds.

### Step 3: Start two simple agent processes

In the second terminal window execute the following command.

$ matrix simpleagent --address 127.0.0.1:16001 --event-db ./events.db --agent-id 1

In the third terminal window execute the following command.

$ matrix simpleagent --address 127.0.0.1:16001 --event-db ./events.db --agent-id 2

The above commands start two simple agent processes. The agent processes
are given the address of the controller process and location of the events file.
The ID of the agents are also specified via command line.

Once all three of the commands have been started, the simulation should run
for ten rounds, and all three processes should exit gracefully.

## The Matrix Simple Agent Demo Code

The code in matrix/simpleagent.py file should serve as a template on how
to write cognitive agent codes. The code of in the file is fairly standalone.
The agent_main function is where the execution starts.

Development of full cognitive agents should be done outside of the matrix
source directory. However, it is recommended that the agents use the same
command line options as the simpleagent command.
