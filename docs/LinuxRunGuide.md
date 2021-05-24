# Runnning MOVES on Linux  
### Author: Joanna Saikali  

## Changes made to original MOVES
- Linux versions of dependency installations (mariaDB, sql, golang)   
- Changes to paths, configurations in several config files   
- Changing MOVES tables & column names to lowercase (very manual process)   
- Created github repository with Linux-friendly code   
- Created setup files that fix Linux permission issues   

## Using my existing AWS server
You will need a credentials file from me. Once you have this file you will be able to SSH into my linux server with a command that I can provide. I will not put it here for security issues, as this is a public repository. Please contact me, Razieh, or Oliver for this.

## How to set up your own Linux server
#### Install MariaDB
```
sudo yum install mariadb #Installing : 1:mariadb-5.5.68-1.amzn2.x86_64
```

#### Create a MariaDB config file
```
sudo vi /etc/yum.repos.d/MariaDB.repo
```

Contents:  
```
[mariadb]
name = MariaDB
baseurl = http://yum.mariadb.org/10.4/centos7-amd64
gpgkey=https://yum.mariadb.org/RPM-GPG-KEY-MariaDB
gpgcheck=1
```

#### Start MariaDB
```
sudo yum install -y MariaDB-server MariaDB-client
sudo yum install mariadb-server
sudo systemctl enable mariadb
sudo systemctl start mariadb
```

#### Install Java
```
sudo amazon-linux-extras install java-openjdk11
sudo yum install java-devel
```

#### Install Go
```
sudo yum install golang -y
```

#### Clone the MOVES repository 
```
mkdir climateaction
cd climateaction
sudo yum install git -y
git clone https://github.com/jsaikali/EPA_MOVES_Model.git
```

#### Move the database Zip file to the right location for setup
```
cd EPA_MOVES_Model/
unzip database/Setup/*.zip -d .
unzip database/Setup/*.zip -d database/Setup/
```
Note: If there is a zip file with new data, you will have had to lowercase all the tables in the zip file.

#### Create necessary directories and change necessary permissions
```
chmod 770 *
chmod 770 ant/bin/ant
mkdir SharedWork 
umask 000 
sudo chown -R mysql WorkerFolder
sudo mkdir /var/lib/WorkerFolder
sudo chmod 777  /var/lib/WorkerFolder
sudo chown -R mysql /var/lib/WorkerFolder
sudo mkdir /var/lib/MOVESTemporary
sudo chmod 777  /var/lib/MOVESTemporary
sudo chown -R mysql /var/lib/MOVESTemporary
```

#### Edit SetupDatabase.sh
This is necessary if you are using new data that was uploaded by the EPA team. This guide was written using the file `movesdb20210209.sql` but the data is updated every 3 months or so.   
Edit the setup file:   
```
vim SetupDatabase.sh
```
Contents:  
```
#!/bin/bash
sudo mysql -uroot -pmoves --force < CreateMOVESUser.sql
sudo mysql -uroot -pmoves < movesdb20210209.sql # OR INSERT YOUR SQL FILE NAME HERE
```

Run setup:  
```
chmod 770 SetupDatabase.sh
bash SetupDatabase.sh
```

## Instructions on running MOVES on Linux server
One can launch a Terminal or Powershell to execute the following commands.   
These were written for terminal but can easily be modified for Powershell.  

#### SSH into the Windows Server
```
ssh -i <pem file> <server login>
```

#### Navigate to MOVES directory and source necessary environment variables
```
cd climateaction/EPA_MOVES_Model
source setenv.csh
```

If you are on a new server rather than Joanna's server, without Linux MOVES cloned, you will need to do the following:
```
git clone https://github.com/jsaikali/EPA_MOVES_Model.git
cd EPA_MOVES_Model 
source setenv.csh
```

#### Compile the MOVES tool
```
ant crungui
```

#### Upload the RunSpec that you would like to run.
Launch a new powershell window and navigate to the directory that contains the RunSpec file you want. Suppose this is called "testrun.mrs".
```
cd /path/to/directory/with/testrun.mrs
sftp -i <pem file> <server login>
cd climateaction/EPA_MOVES_Model
put testrun.mrs
```
Once you have uploaded the desired runspec, you can now close out of this window.  

#### Run the MOVES tool pointing to the runspec desired; write results to log.log
Now in your regular SSH tab, you can execute the RunSpec you uploaded
```
ant run -Drunspec="testrun.mrs" &> log.log
```

#### View results in MariaDB
```
sudo mysql -uroot -pmoves
use database databasetest; # OR WHATEVER YOUR DATABASE IS CALLED BASED ON RUNSPEC
show tables;
select * from movesoutput;
```
