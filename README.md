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
$ conda create -n matrixenv python=3
$ source activate matrixenv
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
You can check if installation was successful with the following command.

```
$ matrix --help
```

## Testing Matrix: Simple Setup - Two dummy agent processes on localhost

For this version of the test we will use two dummy agent processes,
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
state_dsn:
    node1: $HOME/matrixsim/events.db

root_seed: 42
state_store_module: matrix.dummystore
num_rounds: 10
start_time: 2018-06-01
round_time: 1h
```

Now create the initial dummy event database using
the following commands.

```
$ source activate matrixenv
$ matrix dummyagent storeinit -s ~/matrixsim/events.db
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

### Step 5: Start the first dummyagent

Open a *new terminal window* and execute the following commands:

```
$ source activate matrixenv
$ matrix dummyagent start -n node1 -p 16001 -s ~/matrixsim/events.db -i 1 -m 10
```

### Step 6: Start the second dummyagent

Open a *new terminal window* and execute the following commands:

```
$ source activate matrixenv
$ matrix dummyagent start -n node1 -p 16001 -s ~/matrixsim/events.db -i 2 -m 10
```

### Step 7: Cleanup

Wait for the simulation to finish.
All processes except for the RabbitMQ server should exit gracefully.
To stop the RabbitMQ process hit Ctrl-C on the terminal
running RabbitMQ.

## Developing new agents and stores

The Matrix source tarball contains
the dummyagent and dummystore implementations.
These are there to serve as templates for developers
for developing new agents and state store implementations,
and also for testing the Matrix.

The code in matrix/dummyagent.py file should serve as a template
on how to write cognitive agent codes,
while the code in matrix/dummystore.py file should serve as a template
on how to write state store modules.
The code of in these files are fairly standalone.

Development of full cognitive agents should be done
outside of the matrix source directory.
To use dummyagent as a template,
make a copy of dummyagent.py in your development directory,
rename it to reflect the name of your agent process,
and continue working on it as you would for any other python script.
Changes should go into this copy and not into the matrix package.
The same is true for dummystore.py when developing new stores.
Note your agent process code will need to define its own intialization,
and command line argument handling,
that is, the matrix command will not be used to invoke your agent code.

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
