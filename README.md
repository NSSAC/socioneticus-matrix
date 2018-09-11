# Matrix
```

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
```

An agent based modeling framework for social simulation.

## Installation instructions

It is recommended that you install this package
within a virtual environment
created with conda.

### Creating and activating a conda environment

To create a new virtual environment with conda,
have Anaconda/Miniconda setup on your system.
Installation instructions for Anaconda can be found at:
https://conda.io/docs/user-guide/install/index.html
After installation of Anaconda/Miniconda
execute the following commands.

```
$ conda create -n matrixenv -c conda-forge rabbitmq-server python=3
```

### Install RabbitMQ

Execute the following command to install RabbitMQ
within the anaconda environment.

```
$ conda install -c conda-forge rabbitmq-server
```

### Install The Matrix

Copy the current version of the matrix source in the current directory,
and execute the following command.

```
$ pip install ./Matrix-VERSION.tar.gz
```

The above should make the matrix command available.
You can check if installation was successful with the following commands.

```
$ matrix --help
$ bluepill --help
```

## Testing Matrix: Simple Setup - Two BluePill agent processes on localhost

For this version of the test we will use two BluePill agent processes,
that will run on the localhost.

### Step 1: Prepare the work directory

Open a *new terminal window*, and execute the following commands.

```
$ mkdir ~/matrixsim
$ cd ~/matrixsim
```

This will create a folder called matrixsim in your home directory.
Create a file called rabbitmq.conf in the ~/matrixsim using your
favorite text editor, with the following content.

```
default_user = user
default_pass = user
listeners.tcp.1 = 0.0.0.0:5672
```

Also in ~/matrixsim, create a file called matrix.yaml
with the following content.

```
rabbitmq_host: localhost
rabbitmq_port: 5672
rabbitmq_username: user
rabbitmq_password: user
event_exchange: events

sim_nodes:
    - node1
controller_port:
    node1: 16001
num_agentprocs:
    node1: 2
num_storeprocs:
    node1: 1

root_seed: 42
num_rounds: 10
```

Now create the initial BluePill event database using
the following commands.

```
$ source activate matrixenv
$ bluepill store init -s ~/matrixsim/events.db
```

### Step 2: Start RabbitMQ

Open a *new terminal window* and execute the following commands:

```
$ source activate matrixenv
$ matrix rabbitmq start -c ~/matrixsim/rabbitmq.conf -r ~/matrixsim -h localhost
```

### Step 3: Start the event logger

Open a *new terminal window* and execute the following commands:

```
$ source activate matrixenv
$ matrix eventlog -c ~/matrixsim/matrix.yaml -o ~/matrixsim/events.log.gz
```

### Step 4: Start the controller

Open a *new terminal window* and execute the following commands:

```
$ source activate matrixenv
$ matrix controller -c ~/matrixsim/matrix.yaml -n node1
```

### Step 5: Start the BluePill store process

Open a *new terminal window* and execute the following commands:

```
$ source activate matrixenv
$ bluepill store start -s ~/matrixsim/events.db -p 16001 -i 0
```

### Step 6: Start the first BluePill agent process

Open a *new terminal window* and execute the following commands:

```
$ source activate matrixenv
$ bluepill agent start -n node1 -p 16001 -s ~/matrixsim/events.db -i 0 -m 10
```

### Step 7: Start the second BluePill agent process

Open a *new terminal window* and execute the following commands:

```
$ source activate matrixenv
$ bluepill agent start -n node1 -p 16001 -s ~/matrixsim/events.db -i 1 -m 10
```

### Step 8: Cleanup

Wait for the simulation to finish.
All processes except for the RabbitMQ server should exit gracefully.
To stop the RabbitMQ process hit Ctrl-C on the terminal
running RabbitMQ, and wait for it to shutdown cleanly.

## Developing new agents and stores

The Matrix source tarball contains
the bluepill agent and store implementations,
in the following files:
```
bin/bluepill
matrix/client/cli.py
matrix/client/bluepill_agent.py
matrix/client/bluepill_store.py
```

The matrix/client/cli.py implements command line interface
for the bluepill program
and is executed using the bin/bluepill script.

These are there to serve as templates for developers
for developing new agents and state store implementations,
and also for testing the Matrix.

The code in matrix/client/bluepill_agent.py file should serve as a template
on how to write agent codes,
while the code in matrix/client/bluepill_store.py file should serve as a template
on how to write state store modules.
The code of in these files are fairly standalone.

Development of full cognitive agents should be done
outside of the matrix source directory.
To use BluePill agent as a template,
make a copy of bluepill_agent.py in your development directory,
rename it to reflect the name of your agent process,
and continue working on it as you would for any other python script.
Changes should go into this copy and not into the matrix package.
The same is true for bluepill_store.py when developing new stores.
Note your agent process code will need to define its own intialization,
and command line argument handling.
