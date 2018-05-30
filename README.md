# Matrix

                               .              __.....__
                             .'|          .-''         '.
                         .| <  |         /     .-''"'-.  `.
                       .' |_ | |        /     /________\   \
                     .'     || | .'''-. |                  |
                    '--.  .-'| |/.'''. \\    .-------------'
                       |  |  |  /    | | \    '-.____...---.
                       |  |  | |     | |  `.             .'
                       |  '.'| |     | |    `''-...... -'
 __  __   ___          |   / | '.    | '. .--.
|  |/  `.'   `.        `'-'  '---'   '---'|__|
|   .-.  .-.   '              .|  .-,.--. .--.
|  |  |  |  |  |    __      .' |_ |  .-. ||  | ____     _____
|  |  |  |  |  | .:--.'.  .'     || |  | ||  |`.   \  .'    /
|  |  |  |  |  |/ |   \ |'--.  .-'| |  | ||  |  `.  `'    .'
|  |  |  |  |  |`" __ | |   |  |  | |  '- |  |    '.    .'
|__|  |__|  |__| .'.''| |   |  |  | |     |__|    .'     `.
                / /   | |_  |  '.'| |           .'  .'`.   `.
                \ \._,\ '/  |   / |_|         .'   /    `.   `.
                 `--'  `"   `'-'             '----'       '----'

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

$ pip install ./Matrix-VERSION.tar.gz

The above should make the matrix command available.
You can check if installation was successful with the following command.

$ matrix --help

## Testing Matrix: Simple Setup - one controller and two dummy agent processes

Start three separate terminal windows and activate the conda or virtualenv
environment in all of them.

### Step 1: Create the initial events database

In the first terminal window, execute the following command

$ matrix dummystoreinit -s event.db

The above command will create an events database representing global state.

### Step 2: Start the controller process

Execute the following command in the first terminal window.

$ matrix controller -p 16001 -l event.log.gz -s event.db -m matrix.dummystore -n 2 -r 10

The above command will start a controller process
that listens on tcp port 16001 for messages from agent processes,
and writes out events to the event.log.gz file.
When controller receives events it also passes them on to
the matrix.dummystore module which is in charge
of maintaining the current state of the system.
The controller knows there will be two agent processes sending it events
and the simulation will run for 10 rounds.

### Step 3: Start two simple agent processes

In the second terminal window execute the following command.

$ matrix dummyagent -p 16001 -s event.db -i 1 --num-agents 10

In the third terminal window execute the following command.

$ matrix dummyagent -p 16001 -s event.db -i 2 --num-agents 20

The above commands start two dummy agent processes.
The agent processes are given the port of the controller process
and location of the events file.
The ID of the agent process is also specified in the command line.
The first process simulated 10 dummy agents,
while the second simulates 20 dummy agents.

Once all three of the commands have been started, the simulation should run
for ten rounds, and all three processes should exit gracefully.

## The Matrix Dummy Agent Demo Code

The code in matrix/dummyagent.py file should serve as a template
on how to write cognitive agent codes.
The code of in the file is fairly standalone.
The agent_main function is where the execution starts.

Development of full cognitive agents should be done
outside of the matrix source directory.
However, it is recommended that the agents use the same command line options
as the dummyagent command.

## The Matrix Dummy Store Demo Code

The code in matrix/dummystore.py file should serve as a template
on how to write state store modules.
In general the state store module must define one function *get_state_store*
which should accept an URI identifying the state store.
This function should return a state store object
which must implement three methods:
*handle_events*, *flush*, and *close*
with function signatures as in matrix/dummystore.py.

Any store module implementation must not commit
or make the events visible
when the are passed on by *handle_events*.
They should only be committed/made visible
when the subsequent *flush* method is called.
This is so, because we assume that agents cannot see events
produced by other agents till the start of the next simulation round.
The matrix will call the *flush* method at the end of each round
so that this behavior is followed.
